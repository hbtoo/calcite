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
package org.apache.calcite.rel.tvr.rules.dbtoaster;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.List;

import static org.apache.calcite.rel.tvr.rules.dbtoaster.GooJoinOrderingRule.Vertex;
import static org.apache.calcite.rel.tvr.rules.dbtoaster.LoptMultiJoin.Edge;

/**
 * Cost function for join ordering.
 */
interface CostFunction {

  /**
   * Cost estimate.
   * @param vertexes
   * @param relNodes
   * @param multiJoin
   * @param edge
   * @return cost
   */
  double cost(List<Vertex> vertexes,
      List<Pair<RelNode, Mappings.TargetMapping>> relNodes,
      LoptMultiJoin multiJoin, Edge edge);

  /**
   * The bitSet means which input of the MultiJoin has data updated.
   * @param updateInputs
   */
  default void setUpdateInputs(ImmutableBitSet updateInputs) {
  }
}
