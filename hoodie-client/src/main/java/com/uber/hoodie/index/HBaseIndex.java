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

package com.uber.hoodie.index;

import com.google.common.base.Optional;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieDependentSystemUnavailableException;
import com.uber.hoodie.exception.HoodieIndexException;
import com.uber.hoodie.table.HoodieTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Hoodie Index implementation backed by HBase
 */
public class HBaseIndex<T extends HoodieRecordPayload> extends HoodieIndex<T> {
    private final static byte[] SYSTEM_COLUMN_FAMILY = Bytes.toBytes("_s");
    private final static byte[] COMMIT_TS_COLUMN = Bytes.toBytes("commit_ts");
    private final static byte[] FILE_NAME_COLUMN = Bytes.toBytes("file_name");
    private final static byte[] PARTITION_PATH_COLUMN = Bytes.toBytes("partition_path");

    private static Logger logger = LogManager.getLogger(HBaseIndex.class);

    private final String tableName;

    public HBaseIndex(HoodieWriteConfig config, JavaSparkContext jsc) {
        super(config, jsc);
        this.tableName = config.getProps().getProperty(HoodieIndexConfig.HBASE_TABLENAME_PROP);
    }

    @Override
    public JavaPairRDD<HoodieKey, Optional<String>> fetchRecordLocation(
        JavaRDD<HoodieKey> hoodieKeys, HoodieTable<T> hoodieTable) {
        return hoodieKeys.mapPartitionsToPair(new LookUpIndexFunction(hoodieTable), true);
    }

    private static Connection hbaseConnection = null;

    private Connection getHBaseConnection() {
        Configuration hbaseConfig = HBaseConfiguration.create();
        String quorum = config.getProps().getProperty(HoodieIndexConfig.HBASE_ZKQUORUM_PROP);
        hbaseConfig.set("hbase.zookeeper.quorum", quorum);
        String port = config.getProps().getProperty(HoodieIndexConfig.HBASE_ZKPORT_PROP);
        hbaseConfig.set("hbase.zookeeper.property.clientPort", port);
        try {
            return ConnectionFactory.createConnection(hbaseConfig);
        } catch (IOException e) {
            throw new HoodieDependentSystemUnavailableException(
                HoodieDependentSystemUnavailableException.HBASE, quorum + ":" + port);
        }
    }

    /**
     * Function that tags each HoodieKey with an existing location, if known.
     */
    class LookUpIndexFunction
            implements PairFlatMapFunction<Iterator<HoodieKey>, HoodieKey, Optional<String>> {

        private final HoodieTable<T> hoodieTable;
        private final Integer multiGetBatchSize = 100;

        LookUpIndexFunction(HoodieTable<T> hoodieTable) {
            this.hoodieTable = hoodieTable;
        }

        @Override
        public Iterator<Tuple2<HoodieKey, Optional<String>>> call(Iterator<HoodieKey> hoodieKeyIterator) throws Exception {

            // Grab the global HBase connection
            synchronized (HBaseIndex.class) {
                if (hbaseConnection == null) {
                    hbaseConnection = getHBaseConnection();
                }
            }
            List<Tuple2<HoodieKey, Optional<String>>> taggedRecords = new ArrayList<>();
            HTable hTable = null;
            Optional<String> recordLocationPath = Optional.absent();
            try {
                hTable = (HTable) hbaseConnection.getTable(TableName.valueOf(tableName));
                // Do the tagging.
                int keysBatchSize = 0;
                List<Get> keys = new ArrayList<>();
                while (hoodieKeyIterator.hasNext()) {
                    HoodieKey key = hoodieKeyIterator.next();
                    keys.add(new Get(Bytes.toBytes(key.getRecordKey())).setMaxVersions(1)
                            .addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN)
                            .addColumn(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN)
                            .addColumn(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN));
                    if (keysBatchSize < multiGetBatchSize && hoodieKeyIterator.hasNext()) {
                        keysBatchSize++;
                        continue;
                    }
                    keysBatchSize = 0;
                    Result[] results = hTable.get(keys);

                    // first, attempt to grab location from HBase
                    for (Result result : results) {
                        if (result.getRow() != null) {
                            String commitTs =
                                    Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN));
                            String fileId =
                                    Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN));
                            String partitionPath =
                                    Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN));

                            HoodieTimeline commitTimeline = hoodieTable.getCompletedCommitTimeline();
                            // if the last commit ts for this row is less than the system commit ts
                            if (!commitTimeline.empty() && (commitTimeline.containsInstant(
                                    new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTs)) ||
                                    HoodieTimeline.compareTimestamps(commitTimeline.firstInstant().get().getTimestamp(),
                                            commitTs, HoodieTimeline.GREATER))) {
                                recordLocationPath = Optional.of(new Path(
                                        new Path(hoodieTable.getMetaClient().getBasePath(), partitionPath),
                                        fileId).toUri().getPath());
                            }
                        }
                        taggedRecords.add(new scala.Tuple2<>(key, recordLocationPath));
                        keys = new ArrayList<>();
                    }
                }
            } catch (IOException e) {
                throw new HoodieIndexException(
                        "Failed to Tag indexed locations because of exception with HBase Client", e);
            }

            finally {
                if (hTable != null) {
                    try {
                        hTable.close();
                    } catch (IOException e) {
                        // Ignore
                    }
                }

            }
            return taggedRecords.iterator();
        }
    }

    /**
     * Function that tags each HoodieRecord with an existing location, if known.
     */
    class LocationTagFunction
            implements Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>> {

        private final HoodieTable<T> hoodieTable;

        LocationTagFunction(HoodieTable<T> hoodieTable) {
            this.hoodieTable = hoodieTable;
        }

        @Override
        public Iterator<HoodieRecord<T>> call(Integer partitionNum,
                                           Iterator <HoodieRecord<T>> hoodieRecordIterator) {
            // Grab the global HBase connection
            synchronized (HBaseIndex.class) {
                if (hbaseConnection == null) {
                    hbaseConnection = getHBaseConnection();
                }
            }
            List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
            HTable hTable = null;
            try {
                hTable = (HTable) hbaseConnection.getTable(TableName.valueOf(tableName));
                // Do the tagging.
                while (hoodieRecordIterator.hasNext()) {
                    HoodieRecord rec = hoodieRecordIterator.next();
                    // TODO(vc): This may need to be a multi get.
                    Result result = hTable.get(
                            new Get(Bytes.toBytes(rec.getRecordKey())).setMaxVersions(1)
                                    .addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN)
                                    .addColumn(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN)
                                    .addColumn(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN));

                    // first, attempt to grab location from HBase
                    if (result.getRow() != null) {
                        String commitTs =
                                Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN));
                        String fileId =
                                Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN));
                        String partitionPath =
                                Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN));

                        HoodieTimeline commitTimeline = hoodieTable.getCompletedCommitTimeline();
                        // if the last commit ts for this row is less than the system commit ts
                        if (!commitTimeline.empty() && (commitTimeline.containsInstant(
                                new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTs)) ||
                                HoodieTimeline.compareTimestamps(commitTimeline.firstInstant().get().getTimestamp(),
                                        commitTs, HoodieTimeline.GREATER))) {
                            rec = new HoodieRecord(new HoodieKey(rec.getRecordKey(), partitionPath), rec.getData());
                            rec.setCurrentLocation(new HoodieRecordLocation(commitTs, fileId));
                        }
                    }
                    taggedRecords.add(rec);
                }
            } catch (IOException e) {
                throw new HoodieIndexException(
                    "Failed to Tag indexed locations because of exception with HBase Client", e);
            }

            finally {
                if (hTable != null) {
                    try {
                        hTable.close();
                    } catch (IOException e) {
                        // Ignore
                    }
                }

            }
            return taggedRecords.iterator();
        }
    }

    @Override
    public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, HoodieTable<T> hoodieTable) {
        return recordRDD.mapPartitionsWithIndex(this.new LocationTagFunction(hoodieTable), true);
    }

    class UpdateLocationTask implements Function2<Integer, Iterator<WriteStatus>, Iterator<WriteStatus>> {
        @Override
        public Iterator<WriteStatus> call(Integer partition, Iterator<WriteStatus> statusIterator) {

            List<WriteStatus> writeStatusList = new ArrayList<>();
            // Grab the global HBase connection
            synchronized (HBaseIndex.class) {
                if (hbaseConnection == null) {
                    hbaseConnection = getHBaseConnection();
                }
            }
            HTable hTable = null;
            try {
                hTable = (HTable) hbaseConnection.getTable(TableName.valueOf(tableName));
                while (statusIterator.hasNext()) {
                    WriteStatus writeStatus = statusIterator.next();
                    List<Put> puts = new ArrayList<>();
                    List<Delete> deletes = new ArrayList<>();
                    try {
                        for (HoodieRecord rec : writeStatus.getWrittenRecords()) {
                            if (!writeStatus.isErrored(rec.getKey())) {
                                java.util.Optional<HoodieRecordLocation> loc = rec.getNewLocation();
                                if(loc.isPresent()) {
                                    Put put = new Put(Bytes.toBytes(rec.getRecordKey()));
                                    put.addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN,
                                            Bytes.toBytes(loc.get().getCommitTime()));
                                    put.addColumn(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN,
                                            Bytes.toBytes(loc.get().getFileId()));
                                    put.addColumn(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN,
                                            Bytes.toBytes(rec.getPartitionPath()));
                                    puts.add(put);
                                } else {
                                    //Delete existing index for a deleted record
                                    Delete delete = new Delete(Bytes.toBytes(rec.getRecordKey()));
                                    deletes.add(delete);
                                }
                            }
                        }
                        hTable.put(puts);
                        hTable.delete(deletes);
                        hTable.flushCommits();
                    } catch (Exception e) {
                        Exception we = new Exception("Error updating index for " + writeStatus, e);
                        logger.error(we);
                        writeStatus.setGlobalError(we);
                    }
                    writeStatusList.add(writeStatus);
                }
            } catch (IOException e) {
                throw new HoodieIndexException(
                    "Failed to Update Index locations because of exception with HBase Client", e);
            } finally {
                if (hTable != null) {
                    try {
                        hTable.close();
                    } catch (IOException e) {
                        // Ignore
                    }
                }
            }
            return writeStatusList.iterator();
        }
    }

    @Override
    public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD,
        HoodieTable<T> hoodieTable) {
        return writeStatusRDD.mapPartitionsWithIndex(new UpdateLocationTask(), true);
    }

    @Override
    public boolean rollbackCommit(String commitTime) {
        // TODO (weiy)
        return true;
    }
}
