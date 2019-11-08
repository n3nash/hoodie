/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.hadoop.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;

/**
 * Represents a CombineFileSplit for realtime tables
 */
public class HoodieCombineRealtimeFileSplit extends CombineFileSplit {

  // These are instances of HoodieRealtimeSplits
  List<InputSplit> realtimeFileSplits;

  public HoodieCombineRealtimeFileSplit() {
  }

  public HoodieCombineRealtimeFileSplit(JobConf jobConf, List<InputSplit> realtimeFileSplits) {
    super(jobConf, realtimeFileSplits.stream().map(p ->
            ((HoodieRealtimeFileSplit) p).getPath()).collect(Collectors.toList()).toArray(new
            Path[realtimeFileSplits.size()]),
        ArrayUtils.toPrimitive(realtimeFileSplits.stream().map(p -> ((HoodieRealtimeFileSplit) p).getStart())
            .collect(Collectors.toList()).toArray(new Long[realtimeFileSplits.size()])),
        ArrayUtils.toPrimitive(realtimeFileSplits.stream().map(p -> ((HoodieRealtimeFileSplit) p).getLength())
            .collect(Collectors.toList()).toArray(new Long[realtimeFileSplits.size()])),
        realtimeFileSplits.stream().map(p -> {
          try {
            return Arrays.asList(p.getLocations());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }).flatMap(List::stream).collect(Collectors.toList()).toArray(new
            String[realtimeFileSplits.size()]));
    System.out.println("BasePath => " + ((HoodieRealtimeFileSplit)realtimeFileSplits.get(0)).getBasePath());
    System.out.println("Path => " + ((HoodieRealtimeFileSplit)realtimeFileSplits.get(0)).getPath().toString());
    this.realtimeFileSplits = realtimeFileSplits;
  }

  public List<InputSplit> getRealtimeFileSplits() {
    return realtimeFileSplits;
  }

  @Override
  public String toString() {
    return super.toString();
  }

  public static class Builder {

    // These are instances of HoodieRealtimeSplits
    public List<InputSplit> realtimeFileSplits = new ArrayList<>();

    public void addSplit(InputSplit split) {
      realtimeFileSplits.add(split);
    }

    public HoodieCombineRealtimeFileSplit build(JobConf conf) {
      return new HoodieCombineRealtimeFileSplit(conf, realtimeFileSplits);
    }
  }
}
