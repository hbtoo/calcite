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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;
import java.util.Set;

public interface MultiQueryCostOptimizer {

  void relCostIncreased(Set<RelNode> rels);

  boolean tryToMat(Set<RelSubset> subset, MatType matType, boolean strict);

  void rollback();

  void commit();

  Set<RelSubset> getMaterialized();

  RelNode getBest(RelSubset subset);

  RelOptCost potentialBenefit(RelSubset subset, int numReuse);

  /**
   * Gets the overall cost using the given materialized nodes.
   */
  RelOptCost getOverallCost();

  /**
   * Builds the cheapest plan using the given materialized nodes.
   * @param bestNodes
   */
  RelNode buildCheapestPlan(Collection<RelNode> bestNodes);
}
