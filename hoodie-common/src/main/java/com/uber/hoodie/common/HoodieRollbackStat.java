/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common;

import org.apache.hadoop.fs.FileStatus;

import java.io.File;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Collects stats about a single partition clean operation
 */
public class HoodieRollbackStat implements Serializable {
    // Partition path
    private final String partitionPath;
    private final List<String> successDeleteFiles;
    // Files that could not be deleted
    private final List<String> failedDeleteFiles;
    private final Map<FileStatus, Long> commandBlocks;

    public HoodieRollbackStat(String partitionPath, List<String> successDeleteFiles,
        List<String> failedDeleteFiles, Map<FileStatus, Long> commandBlocks) {
        this.partitionPath = partitionPath;
        this.successDeleteFiles = successDeleteFiles;
        this.failedDeleteFiles = failedDeleteFiles;
        this.commandBlocks = commandBlocks;
    }

    public Map<FileStatus, Long> getCommandBlocks() {
        return commandBlocks;
    }

    public String getPartitionPath() {
        return partitionPath;
    }

    public List<String> getSuccessDeleteFiles() {
        return successDeleteFiles;
    }

    public List<String> getFailedDeleteFiles() {
        return failedDeleteFiles;
    }

    public static HoodieRollbackStat.Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private List<String> successDeleteFiles;
        private List<String> failedDeleteFiles;
        private Map<FileStatus, Long> commandBlocks;
        private String partitionPath;

        public Builder withDeletedFileResults(Map<FileStatus, Boolean> deletedFiles) {
            //noinspection Convert2MethodRef
            successDeleteFiles = deletedFiles.entrySet().stream().filter(s -> s.getValue())
                .map(s -> s.getKey().getPath().toString()).collect(Collectors.toList());
            failedDeleteFiles = deletedFiles.entrySet().stream().filter(s -> !s.getValue())
                .map(s -> s.getKey().getPath().toString()).collect(Collectors.toList());
            return this;
        }

        public Builder withRollbackBlockAppendResults(Map<FileStatus, Long> commandBlocks) {
            this.commandBlocks = commandBlocks;
            return this;
        }

        public Builder withPartitionPath(String partitionPath) {
            this.partitionPath = partitionPath;
            return this;
        }

        public HoodieRollbackStat build() {
            return new HoodieRollbackStat(partitionPath, successDeleteFiles, failedDeleteFiles, commandBlocks);
        }
    }
}
