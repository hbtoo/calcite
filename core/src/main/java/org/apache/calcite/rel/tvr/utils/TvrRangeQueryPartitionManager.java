/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.tvr.utils;

import org.apache.calcite.plan.RelOptTable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Manage the mapping between table partition and tvr version.
 */
public class TvrRangeQueryPartitionManager {

  // <Table name, <Tvr version, Qualified partitions>>
  private Map<String, Map<Long, Set<String>>> partitionVersionMapping = new HashMap<>();

  private static final Log LOG = LogFactory.getLog(TvrRangeQueryPartitionManager.class);

  /**
   *  convert the partitions in the input tables to the tvr versions.
   */
  public void recordTablePartitions(Set<RelOptTable> tables) {
    throw new UnsupportedOperationException("recordTablePartitions not supported");
  }

}
