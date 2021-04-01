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

import org.apache.calcite.plan.InterTvrRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty.PropertyType.DB_TOASTER;

public class TvrDBTPropRule extends RelOptTvrRule implements InterTvrRule {

  public static final TvrDBTPropRule FILTER_INSTANCE = new TvrDBTPropRule(LogicalFilter.class);
  public static final TvrDBTPropRule PROJECT_INSTANCE = new TvrDBTPropRule(LogicalProject.class);
  public static final TvrDBTPropRule AGGREGATE_INSTANCE =
      new TvrDBTPropRule(LogicalAggregate.class);
  public static final TvrDBTPropRule SORT_INSTANCE = new TvrDBTPropRule(LogicalSort.class);
  public static final TvrDBTPropRule WINDOW_INSTANCE = new TvrDBTPropRule(LogicalWindow.class);
  public static final TvrDBTPropRule MULTI_JOIN_INSTANCE = new TvrDBTPropRule(MultiJoin.class);
  public static final TvrDBTPropRule UNION_INSTANCE = new TvrDBTPropRule(Union.class);

  private static RelOptRuleOperand getRootOperand(Class<? extends RelNode> relClass) {
    Supplier<RelOptRuleOperand> child = () -> operand(
        RelSubset.class, tvrEdgeSSMax(
        tvr(tvrProperty(TvrUpdateOneTableProperty.class, tvr(tvrEdgeSSMax(logicalSubset()))))),
        any());
    if (SingleRel.class.isAssignableFrom(relClass)) {
      return operand(relClass, tvrEdgeSSMax(tvr(false)), some(child.get()));
    } else {
      return operand(relClass, tvrEdgeSSMax(tvr(false)), unordered(child.get()));
    }
  }

  public TvrDBTPropRule(Class<? extends RelNode> relClass) {
    super(getRootOperand(relClass), "TvrDBTPropRule-" + relClass.getSimpleName());
  }

  @Override public boolean matches(RelOptRuleCall call) {
    VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();

    RelMatch root = getRoot(call);
    RelNode rootRel = root.get();
    TvrMetaSet rootTvr = root.tvr().get();

    RelMatch matchedInput = root.input(0);
    TvrMetaSet inputTvr = matchedInput.tvr().get();
    TvrUpdateOneTableProperty tvrProp = matchedInput.tvr().property();

    // must match DB_TOASTER tvr property
    if (tvrProp.getType() != DB_TOASTER) {
      return false;
    }

    // root tvr and input tvr type must be the same
    if (!rootTvr.getTvrType().equals(inputTvr.getTvrType())) {
      return false;
    }

    for (int i = 0; i < rootRel.getInputs().size(); i++) {
      RelNode input = rootRel.getInput(i);
      // find the set snapshot max tvrs of this input
      Set<TvrMetaSet> tvrMetaSets =
          planner.getSubset(input).getTvrLinks().get(TvrSetSnapshot.SET_SNAPSHOT_MAX);
      if (tvrMetaSets.isEmpty()) {
        return false;
      }
      // find the tvr with the same tvr type
      Optional<TvrMetaSet> tvr =
          tvrMetaSets.stream().filter(t -> t.getTvrType().equals(rootTvr.getTvrType())).findFirst();
      if (!tvr.isPresent()) {
        return false;
      }
      // find the sibling tvr with the same dbtoaster property
      Map<TvrUpdateOneTableProperty, TvrMetaSet> propLinks = tvr.get()
          .getTvrPropertyLinks(p -> p instanceof TvrUpdateOneTableProperty && tvrProp.equals(p));
      if (propLinks.isEmpty()) {
        return false;
      }
      assert propLinks.size() == 1;
    }

    return true;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    RelNode rootOp = root.get();
    TvrMetaSet rootTvr = root.tvr().get();

    VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();
    RelOptCluster cluster = rootOp.getCluster();

    TvrUpdateOneTableProperty property = root.input(0).tvr().property();
    assert property.getType() == DB_TOASTER;
    TvrMetaSetType updateOneTableTvrType = TvrPropertyUtil
        .getDbtUpdateOneTableTvrType(property.getChangingTable(), rootTvr.getTvrType());
    assert updateOneTableTvrType != TvrMetaSetType.DEFAULT;

    List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input : rootOp.getInputs()) {
      Set<TvrMetaSet> tvrMetaSets =
          planner.getSubset(input).getTvrLinks().get(TvrSetSnapshot.SET_SNAPSHOT_MAX);
      assert !tvrMetaSets.isEmpty();
      Optional<TvrMetaSet> tvr =
          tvrMetaSets.stream().filter(t -> t.getTvrType().equals(rootTvr.getTvrType())).findFirst();
      assert tvr.isPresent();
      Map<TvrUpdateOneTableProperty, TvrMetaSet> propLinks = tvr.get()
          .getTvrPropertyLinks(p -> p instanceof TvrUpdateOneTableProperty && property.equals(p));
      assert !propLinks.isEmpty();
      assert propLinks.size() == 1;
      TvrMetaSet updateTvr = propLinks.values().iterator().next();

      RelNode relNode = updateTvr.getSubset(TvrSemantics.SET_SNAPSHOT_MAX, cluster.traitSet());
      newInputs.add(relNode);
      assert relNode != null;
    }

    RelNode updateRel = rootOp.copy(rootOp.getTraitSet(), newInputs);

    RelOptRuleCall.TransformBuilder builder = call.transformBuilder();
    builder.addPropertyLink(rootTvr, property, updateRel, updateOneTableTvrType);
    builder.transform();
  }

}
