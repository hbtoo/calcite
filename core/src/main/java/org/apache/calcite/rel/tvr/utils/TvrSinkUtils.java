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

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.MatType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.temp.PhysicalTableSink;
import org.apache.calcite.rel.temp.PhysicalTableSpool;

import java.util.List;
import java.util.Map;

import static org.apache.calcite.plan.volcano.MatType.MAT_DISK;
import static org.apache.calcite.plan.volcano.MatType.MAT_MEMORY;

public class TvrSinkUtils {

  private TvrSinkUtils(){}

  public static RelNode buildMatSink(RelNode rel, MatType matType, RelTraitSet traitSet,
      String projectName, String tableName, double estimatedRowCount,
      Map<String, String> parameters) {
    assert matType == MAT_MEMORY || matType == MAT_DISK;

    return new PhysicalTableSpool(rel.getTraitSet(), rel, projectName, tableName);

  }

  public static PhysicalTableSink buildSinkForDownStream(RelNode input,
      PhysicalTableSink relatedSink, MatType matType, RelTraitSet traitSet, String projectName,
      String tableName, double estimatedRowCount) {

    throw new UnsupportedOperationException("buildSinkForDownStream not supported");
  }

  public static void addHardLinks(PhysicalTableSink sink, List<PhysicalTableSink> linkedSinks) {
    throw new UnsupportedOperationException("addHardLink not supported");
  }

}
