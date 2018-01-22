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

package com.uber.hoodie.common.util.collection;


import com.uber.hoodie.common.model.HoodieAvroPayload;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.common.util.SchemaTestUtil;
import com.uber.hoodie.common.util.SpillableMapTestUtils;
import org.apache.avro.generic.IndexedRecord;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

// To run : mvn clean test -Dtest=com.uber.hoodie.common.util.collection.TestSuiteForSpillableMap#testLargeInsertUpsert -Dmultiple_factor=100 -Dnum_records=100000 -Dnum_iterations=100
// Total inserts = multiple_factor*num_records
// num_iterations = number of times to run the iterator to get a long running job to see performance
public class TestSuiteForSpillableMap {

  @Test
  public void testLargeInsertUpsert() throws Exception, URISyntaxException {
    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>
            (16L, HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema()),
                HoodieAvroPayload.class.getName(), Optional.empty()); //16B

    long startTime = System.currentTimeMillis();
    for(int i = 0; i < Integer.parseInt(System.getProperty("multiple_factor")) ; i++) {
    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, Integer.parseInt(System.getProperty("num_records")));
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
    }
    for(int i = 0; i < Integer.parseInt(System.getProperty("num_iterations")); i++) {
      Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
      List<HoodieRecord> oRecords = new ArrayList<>();
      while (itr.hasNext()) {
        HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
        oRecords.add(rec);
        //records.get(rec.getRecordKey());
      }
    }
    System.out.println("Time taken " + (System.currentTimeMillis() - startTime));
  }
}
