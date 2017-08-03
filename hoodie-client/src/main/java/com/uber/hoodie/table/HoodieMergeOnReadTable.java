/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.table;

import com.google.common.collect.Sets;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieRollbackStat;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieCompactionMetadata;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieCompactionException;
import com.uber.hoodie.io.HoodieAppendHandle;
import com.uber.hoodie.io.compact.HoodieRealtimeTableCompactor;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Implementation of a more real-time read-optimized Hoodie Table where
 *
 * INSERTS - Same as HoodieCopyOnWriteTable - Produce new files, block aligned to desired size (or)
 *           Merge with the smallest existing file, to expand it
 *
 * UPDATES - Appends the changes to a rolling log file maintained per file Id.
 * Compaction merges the log file into the base file.
 *
 */
public class HoodieMergeOnReadTable<T extends HoodieRecordPayload> extends HoodieCopyOnWriteTable<T> {
    private static Logger logger = LogManager.getLogger(HoodieMergeOnReadTable.class);

    public HoodieMergeOnReadTable(HoodieWriteConfig config,
        HoodieTableMetaClient metaClient) {
        super(config, metaClient);
    }

    @Override
    public Iterator<List<WriteStatus>> handleUpdate(String commitTime, String fileId,
        Iterator<HoodieRecord<T>> recordItr) throws IOException {
        logger.info("Merging updates for commit " + commitTime + " for file " + fileId);
        HoodieAppendHandle<T> appendHandle =
            new HoodieAppendHandle<>(config, commitTime, this, fileId, recordItr);
        appendHandle.doAppend();
        appendHandle.close();
        return Collections.singletonList(Collections.singletonList(appendHandle.getWriteStatus()))
            .iterator();
    }

    @Override
    public Optional<HoodieCompactionMetadata> compact(JavaSparkContext jsc) {
        logger.info("Checking if compaction needs to be run on " + config.getBasePath());
        Optional<HoodieInstant> lastCompaction = getActiveTimeline().getCompactionTimeline()
            .filterCompletedInstants().lastInstant();
        String deltaCommitsSinceTs = "0";
        if (lastCompaction.isPresent()) {
            deltaCommitsSinceTs = lastCompaction.get().getTimestamp();
        }

        int deltaCommitsSinceLastCompaction = getActiveTimeline().getDeltaCommitTimeline()
            .findInstantsAfter(deltaCommitsSinceTs, Integer.MAX_VALUE).countInstants();
        if (config.getInlineCompactDeltaCommitMax() > deltaCommitsSinceLastCompaction) {
            logger.info("Not running compaction as only " + deltaCommitsSinceLastCompaction
                + " delta commits was found since last compaction " + deltaCommitsSinceTs
                + ". Waiting for " + config.getInlineCompactDeltaCommitMax());
            return Optional.empty();
        }

        logger.info("Compacting merge on read table " + config.getBasePath());
        HoodieRealtimeTableCompactor compactor = new HoodieRealtimeTableCompactor();
        try {
            return Optional.of(compactor.compact(jsc, config, this));
        } catch (IOException e) {
            throw new HoodieCompactionException("Could not compact " + config.getBasePath(), e);
        }
    }

    @Override
    public List<HoodieRollbackStat> rollback(JavaSparkContext jsc, List<String> commits) throws IOException {

        Map<String, HoodieInstant> commitsAndCompactions =
                this.getActiveTimeline().getInstants().filter(i ->
                        Sets.newHashSet(HoodieActiveTimeline.COMMIT_ACTION, HoodieActiveTimeline.COMPACTION_ACTION, HoodieActiveTimeline.DELTA_COMMIT_ACTION).contains(i.getAction()))
                        .filter(i -> commits.contains(i.getTimestamp())).collect(Collectors.toMap(i -> i.getTimestamp(), i -> i));

        // Atomically un-publish all non-inflight commits
        commitsAndCompactions.entrySet().stream().map(entry -> entry.getValue())
                .filter(i -> !i.isInflight()).forEach(this.getActiveTimeline()::revertToInflight);

        logger.info("Unpublished " + commits);

        // Parallelize in case we want to actually delete files/log blocks, now just revert <action>.commit and write a .rollback file
        List<HoodieRollbackStat> allRollbackStats = jsc.parallelize
                (FSUtils.getAllPartitionPaths(FSUtils.getFs(), this.getMetaClient().getBasePath(), config.shouldAssumeDatePartitioning()))
                .map((Function<String, List<HoodieRollbackStat>>) partitionPath -> {
                    return commits.stream().map(commit -> {
                        HoodieInstant instant = commitsAndCompactions.get(commit);
                        HoodieRollbackStat hoodieRollbackStats = null;
                        switch (instant.getAction()) {
                            case HoodieTimeline.COMMIT_ACTION: case HoodieTimeline.COMPACTION_ACTION:
                                try {
                                    Map<FileStatus, Boolean> results = super.delete(partitionPath, Arrays.asList(commit));
                                    hoodieRollbackStats = HoodieRollbackStat.newBuilder().withPartitionPath(partitionPath)
                                            .withDeletedFileResults(results).build();
                                    break;
                                } catch (IOException io) {
                                    throw new UncheckedIOException("Failed to rollback for commit " + commit, io);
                                }
                            case HoodieTimeline.DELTA_COMMIT_ACTION:
                                try {
                                    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                                            .fromBytes(this.getCommitTimeline().getInstantDetails(new HoodieInstant(true, instant.getAction(), instant.getTimestamp())).get());

                                    Map<FileStatus, Boolean> filesToDeletedStatus = new HashMap<>();
                                    Map<FileStatus, Integer> filesToNumBlocksRollback = new HashMap<>();

                                    commitMetadata.getPartitionToWriteStats().get(partitionPath).stream().forEach(wStat -> {
                                        try {
                                            boolean isLogFile = FSUtils.getFileExtension(wStat.getPath()).contains(HoodieLogFile.DELTA_EXTENSION);
                                            // delta commit can have updates + inserts
                                            if (isLogFile) {
                                                int numberOfCommandBlocks = 0;
                                                numberOfCommandBlocks++;
                                                if (wStat.getNumDeletes() > 0)
                                                    numberOfCommandBlocks++;
                                                filesToNumBlocksRollback.put(FSUtils.getFs().getFileStatus(new Path(this.getMetaClient().getBasePath(), wStat.getPath())), numberOfCommandBlocks);
                                            } else {
                                                // delete parquet files
                                                FileSystem fs = FSUtils.getFs();
                                                Path toDelete = new Path(this.getMetaClient().getBasePath(), wStat.getPath());
                                                FileStatus f = fs.getFileStatus(toDelete);
                                                Boolean deleted = fs.delete(toDelete, false);
                                                filesToDeletedStatus.put(f, deleted);
                                            }
                                        } catch(IOException io) {
                                            throw new UncheckedIOException("Failed to collect rollback stats for commit " + commit, io);
                                        }
                                    });
                                    hoodieRollbackStats = HoodieRollbackStat.newBuilder().withPartitionPath(partitionPath)
                                            .withDeletedFileResults(filesToDeletedStatus)
                                            .withRollbackBlockAppendResults(filesToNumBlocksRollback).build();
                                    break;
                                } catch (IOException io) {
                                    throw new UncheckedIOException("Failed to rollback for commit " + commit, io);
                                }
                        }
                        return hoodieRollbackStats;
                    }).collect(Collectors.toList());
                }).flatMap(x -> x.iterator()).collect();

        commitsAndCompactions.entrySet().stream()
                .map(entry -> new HoodieInstant(true, entry.getValue().getAction(), entry.getValue().getTimestamp()))
                .forEach(this.getActiveTimeline()::deleteInflight);

        return allRollbackStats;
    }

}
