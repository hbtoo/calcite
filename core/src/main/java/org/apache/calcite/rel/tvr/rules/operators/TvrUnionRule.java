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
package org.apache.calcite.rel.tvr.rules.operators;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSemantics;
import org.apache.calcite.plan.volcano.RelSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TvrUnionRule extends RelOptTvrRule {

  public static final TvrUnionRule INSTANCE = new TvrUnionRule();

  /**
   * Matches:
   * <p>
   *   LogicalUnion --SET_SNAPSHOT_MAX-- Tvr0
   *          |
   *          |                                                (ValueDelta)
   *    ManyInputs  --SET_SNAPSHOT_MAX-- Tvr(1, 2 ...)  ----------------------  ManyInputs
   * <p>
   * Converts to:
   * <p>
   *                                                           (ValueDelta)
   *     LogicalUnion --SET_SNAPSHOT_MAX-- Tvr0       --------------------------   newUnion
   *          |                                                                       |
   *          |                                                (ValueDelta)           |
   *      ManyInputs  --SET_SNAPSHOT_MAX-- Tvr(1, 2 ...)  ----------------------  ManyInputs
   *
   * <p>
   */
  private TvrUnionRule() {
    super(
        operand(LogicalUnion.class, tvrEdgeSSMax(tvr()),
        unordered(
            operand(
                RelSubset.class, tvrEdgeSSMax(
                    tvr(
            // let TvrSetDeltaUnionRule handle set delta
            tvrEdge(TvrSemantics.class, x -> !(x instanceof TvrSetSemantics),
                logicalSubset()))), any()))));
  }

  @Override public boolean matches(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    LogicalUnion union = root.get();
    VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();
    RelOptCluster cluster = union.getCluster();

    TvrMetaSet childTvr = root.input(0).tvr().get();
    TvrSemantics tvrTrait = root.input(0).tvrSibling().tvrSemantics;

    // The input matched is ready, but we fire only when all inputs are ready
    List<RelSubset> newInputs = union.getInputs().stream().map(input -> {
      RelSet set = planner.getSet(input);
      TvrMetaSet myChildTvr = set.getTvrForTvrSet(childTvr.getTvrType());
      if (myChildTvr == null) {
        return null;
      }
      return myChildTvr.getSubset(tvrTrait, cluster.traitSet());
    }).filter(Objects::nonNull).collect(Collectors.toList());

    if (newInputs.size() != union.getInputs().size()) {
      return false;
    }

    // Sometimes, these inputs with value semantics do not have the same data types
    // before they have been transformed to set semantics.
    RelDataType dataType = newInputs.get(0).getRowType();
    return newInputs.stream().skip(1).allMatch(
        r -> RelOptUtil.areRowTypesEqual(r.getRowType(), dataType, false));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    LogicalUnion union = root.get();
    VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();
    RelOptCluster cluster = union.getCluster();

    TvrMetaSetType tvrType = root.input(0).tvr().get().getTvrType();
    TvrSemantics tvrTrait = root.input(0).tvrSibling().tvrSemantics;

    List<RelNode> newInputs = union.getInputs().stream().map(input -> {
      RelSet set = planner.getSet(input);
      TvrMetaSet myChildTvr = set.getTvrForTvrSet(tvrType);
      assert myChildTvr != null;
      RelNode newInput = myChildTvr.getSubset(tvrTrait, cluster.traitSet());
      assert newInput != null;
      return newInput;
    }).collect(Collectors.toList());

    // different inputs might have different nullable properties
    // we need derive row type first
    RelNode newUnion = union.copy(union.getTraitSet(), newInputs);
    RelDataType rowType = newUnion.getRowType();
    newUnion = call.builder().push(newUnion).convert(rowType, true).build();

    transformToRootTvr(call, newUnion, tvrTrait);
  }

}
