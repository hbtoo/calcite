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
package org.apache.calcite.rel.tvr.rules.outerjoinview;

import org.apache.calcite.plan.InterTvrRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.volcano.RelSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrPropertyEdgeRuleOperand;
import org.apache.calcite.plan.volcano.TvrRelOptRuleOperand;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.tvr.trait.property.TvrOuterJoinViewProperty;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TvrOjvGooJoinOrderRule extends RelOptTvrRule
    implements InterTvrRule {

  public static final TvrOjvGooJoinOrderRule INSTANCE = new TvrOjvGooJoinOrderRule();

  private static RelOptRuleOperand getOp() {
    // A tvr with a tvr property self loop
    TvrPropertyEdgeRuleOperand propertyEdge =
        tvrProperty(TvrOuterJoinViewProperty.class, tvr());
    TvrRelOptRuleOperand tvr = tvr(propertyEdge);
    propertyEdge.setToTvrOp(tvr);

    return operand(MultiJoin.class, tvrEdgeSSMax(tvr()),
        unordered(operand(RelSubset.class, tvrEdgeSSMax(tvr), any())));
  }

  private TvrOjvGooJoinOrderRule() {
    super(getOp());
  }

  @Override public boolean matches(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    MultiJoin join = getRoot(call).get();
    VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();

    TvrMetaSet rootTvr = root.tvr().get();
    Map<RelNode, TvrOuterJoinViewProperty> childProperties = new HashMap<>();
    boolean ready = join.getInputs().stream().allMatch(input -> {
      RelSet set = planner.getSet(input);
      TvrMetaSet childTvr = set.getTvrForTvrSet(rootTvr.getTvrType());
      Map<TvrOuterJoinViewProperty, TvrMetaSet> map =
          childTvr.getTvrPropertyLinks(TvrOuterJoinViewProperty.class);
      if (map.size() > 0) {
        childProperties.put(input, map.keySet().iterator().next());
      }
      return map.size() > 0;
    });

    // Not all inputs have TvrOuterJoinViewProperty yet, wait for next match
    if (!ready) {
      return false;
    }

    // inputs should have distinct changing tables
    Set<Integer> tables = new HashSet<>();
    return childProperties.values().stream().allMatch(childProperty -> {
      if (tables.removeAll(childProperty.allChangingTermTables())) {
        return false;
      }
      tables.addAll(childProperty.allChangingTermTables());
      return true;
    });
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    MultiJoin multiJoin = getRoot(call).get();
    TvrMetaSet rootTvr = root.tvr().get();
    VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();

    // Generate the join order where each input comes first
    int index = -1;
    for (RelNode input : multiJoin.getInputs()) {
      index++;
      RelSet set = planner.getSet(input);
      TvrMetaSet childTvr = set.getTvrForTvrSet(rootTvr.getTvrType());

      Map<TvrOuterJoinViewProperty, TvrMetaSet> map =
          childTvr.getTvrPropertyLinks(TvrOuterJoinViewProperty.class);
      TvrOuterJoinViewProperty childProperty = map.keySet().iterator().next();
      // If this input has no changing tables, don't generate the pre order
      // for this input
      if (childProperty.allChangingTermTables().isEmpty()) {
        continue;
      }

//      UpdateFirstCostFunction costFunc = new UpdateFirstCostFunction();
//      costFunc.setUpdateInputs(ImmutableBitSet.of(index));
//      RelNode newJoin = new GooJoinOrderingRule(false, costFunc,
//          "TvrOjvGooJoinOrderingRule-temp")
//          .transform(multiJoin, call.builder());
//
//      call.transformBuilder().addEquiv(newJoin, multiJoin)
//          .addTvrType(newJoin, rootTvr.getTvrType()).transform();
    }
  }
}
