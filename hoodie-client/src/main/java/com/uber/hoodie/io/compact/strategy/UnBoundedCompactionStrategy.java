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

package com.uber.hoodie.io.compact.strategy;

import com.uber.hoodie.avro.model.HoodieCompactionOperation;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.config.HoodieWriteConfig;
import java.util.List;

/**
 * UnBoundedCompactionStrategy will not change ordering or filter any compaction. It is a
 * pass-through and will compact all the base files which has a log file. This usually means
 * no-intelligence on compaction.
 *
 * @see CompactionStrategy
 */
public class UnBoundedCompactionStrategy extends CompactionStrategy {

  @Override
  public List<HoodieCompactionOperation> orderAndFilter(HoodieWriteConfig config,
      List<HoodieCompactionOperation> operations, List<HoodieCompactionPlan> pendingCompactionWorkloads) {
    return operations;
  }
}
