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
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.table.HoodieTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestHbaseIndex {

    private JavaSparkContext jsc = null;
    private String basePath = null;
    private transient final FileSystem fs;
    private static HBaseTestingUtility utility;
    private static Configuration hbaseConfig;
    private static String tableName = "test_table";

    @After
    public void clean() throws Exception {
        if (basePath != null) {
            new File(basePath).delete();
        }
        if (jsc != null) {
            jsc.stop();
        }
        if(utility != null) {
            utility.shutdownMiniCluster();
        }
    }

    public TestHbaseIndex() throws Exception {
        fs = FSUtils.getFs();
    }

    @Before
    public void init() throws Exception {

        // Initialize HbaseMiniCluster
        utility = new HBaseTestingUtility();
        utility.startMiniCluster();
        hbaseConfig = utility.getConnection().getConfiguration();
        utility.createTable(TableName.valueOf(tableName), Bytes.toBytes("_s"));

        // Initialize a local spark env
        SparkConf sparkConf = new SparkConf().setAppName("TestHbaseIndex").setMaster("local[1]");
        jsc = new JavaSparkContext(sparkConf);
        // Create a temp folder as the base path
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        basePath = folder.getRoot().getAbsolutePath();
        HoodieTestUtils.init(basePath);
    }

    @Test
    public void testTagUpdateAndFetchLocation() throws Exception {

        String newCommitTime = "001";
        HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

        // Load to memory
        HoodieWriteConfig config = getConfig();
        HBaseIndex index = new HBaseIndex(config, jsc);

        HoodieWriteClient writeClient = new HoodieWriteClient(jsc, config);
        HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs, basePath);
        HoodieTable hoodieTable = HoodieTable.getHoodieTable(metaClient, config);

        // Test tagLocation without any entries in index
        JavaRDD<HoodieRecord> javaRDD = index.tagLocation(writeRecords, hoodieTable);
        assert(javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 0);

        // Insert 200 records
        JavaRDD<WriteStatus> writeStatues = writeClient.upsert(writeRecords, newCommitTime);
        assertNoWriteErrors(writeStatues.collect());

        // Update location of records inserted and validate no errors
        writeStatues = index.updateLocation(writeStatues, hoodieTable);
        assertNoWriteErrors(writeStatues.collect());

        // Now tagLocation for these records, hbaseIndex should tag them correctly
        javaRDD = index.tagLocation(writeRecords, hoodieTable);
        assert(javaRDD.filter(record -> record.isCurrentLocationKnown()).collect().size() == 200);

        List<String> partitionsPaths = records.stream().map(record -> record.getPartitionPath())
                .collect(Collectors.toList());

        // Fetch location for HoodieKeys inserted, they should be tagged with the correct partitionPaths from Hbase
        records = records.stream().map(rec -> new HoodieRecord(new HoodieKey(rec.getRecordKey(), "wrong_partition_path"), rec.getData())).collect(Collectors.toList());
        writeRecords = jsc.parallelize(records, 1);

        JavaPairRDD<HoodieKey, Optional<String>> existsRDD = index.fetchRecordLocation(writeRecords.map(rec -> rec.getKey()), hoodieTable);
        assert(existsRDD.filter(record -> record._2().isPresent()).collect().size() == 200);
        List<String> fetchedPartitionPaths = existsRDD.map(rec2 -> {
            String path = rec2._2().get();
            path = path.substring(0, path.lastIndexOf("/"));
            String day = path.substring(path.lastIndexOf("/"), path.length());
            path = path.substring(0, path.lastIndexOf("/"));
            String month = path.substring(path.lastIndexOf("/"), path.length());
            path = path.substring(0, path.lastIndexOf("/"));
            String year = path.substring(path.lastIndexOf("/"), path.length());
            path = path.substring(0, path.lastIndexOf("/"));
            return year.substring(1, year.length()) + month + day;
        }).collect();

        assertEquals(partitionsPaths, fetchedPartitionPaths);

    }

    private void assertNoWriteErrors(List<WriteStatus> statuses) {
        // Verify there are no errors
        for (WriteStatus status : statuses) {
            assertFalse("Errors found in write of " + status.getFileId(), status.hasErrors());
        }
    }

    private HoodieWriteConfig getConfig() {
        return getConfigBuilder().build();
    }

    private HoodieWriteConfig.Builder getConfigBuilder() {
        return HoodieWriteConfig.newBuilder().withPath(basePath)
                .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
                .withCompactionConfig(
                        HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024)
                                .withInlineCompaction(false).build())
                .withStorageConfig(HoodieStorageConfig.newBuilder().limitFileSize(1024 * 1024).build())
                .forTable("test-trip-table").withIndexConfig(
                        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.HBASE)
                                .hbaseZkPort(Integer.valueOf(hbaseConfig.get("hbase.zookeeper.property.clientPort")))
                                .hbaseZkQuorum(hbaseConfig.get("hbase.zookeeper.quorum")).hbaseTableName(tableName).build());
    }
}
