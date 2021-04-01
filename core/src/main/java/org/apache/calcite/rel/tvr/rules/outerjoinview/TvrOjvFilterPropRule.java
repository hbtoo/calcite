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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrPropertyEdgeRuleOperand;
import org.apache.calcite.plan.volcano.TvrRelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.tvr.trait.property.TvrOuterJoinViewProperty;
import org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;

import java.util.Map;

import static org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil.updateOneTableProperties;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.matchNullable;


public class TvrOjvFilterPropRule extends RelOptTvrRule
    implements InterTvrRule {

  public static final TvrOjvFilterPropRule INSTANCE = new TvrOjvFilterPropRule();

  private static RelOptRuleOperand getOp() {
    // A tvr with a tvr property self loop
    TvrPropertyEdgeRuleOperand propertyEdge =
        tvrProperty(TvrOuterJoinViewProperty.class, tvr());
    TvrRelOptRuleOperand tvr = tvr(propertyEdge);
    propertyEdge.setToTvrOp(tvr);

    return operand(LogicalFilter.class, tvrEdgeSSMax(tvr()),
        operand(RelSubset.class, tvrEdgeSSMax(tvr), any()));
  }

  private TvrOjvFilterPropRule() {
    super(getOp());
  }

  @Override public void onMatch(RelOptRuleCall call) {
    LogicalFilter filter = getRoot(call).get();
    TvrMetaSet tvr = getRoot(call).tvr().get();
    TvrMatch childTvr = getRoot(call).input(0).tvr();

    TvrOuterJoinViewProperty childOjvProperty =
        childTvr.property(TvrOuterJoinViewProperty.class);
    Map<TvrUpdateOneTableProperty, TvrMetaSet> childVDProperties =
        updateOneTableProperties(childTvr.get(), TvrUpdateOneTableProperty.PropertyType.OJV);

    RelOptCluster cluster = filter.getCluster();
    // LinkedHashMap are used to preserve register order, especially that
    // TvrOuterJoinViewProperty is registered last
    RelOptRuleCall.TransformBuilder builder = call.transformBuilder();

    // Compute all new VD property links
    childVDProperties.forEach((vdProperty, toTvr) -> {
      RelSubset child =
          toTvr.getSubset(TvrSemantics.SET_SNAPSHOT_MAX, cluster.traitSet());
      assert child != null;
      RelNode newRel = copyWithNewInputType(filter, child, cluster, call);
      builder.addPropertyLink(tvr, vdProperty, newRel, toTvr.getTvrType());
    });

    TvrOuterJoinViewProperty newOjvProperty =
        new TvrOuterJoinViewProperty(childOjvProperty.getTerms(),
            childOjvProperty.getNonNullTermTables(), childOjvProperty.allChangingTermTables());
    assert TvrPropertyUtil.checkExisitingOJVProperty(call, tvr, filter, newOjvProperty)
        : "TvrOuterJoinViewProperty doesn't match at " + filter.getId();

    // Add the TvrOuterJoinViewProperty self loop on root tvr
    builder.addPropertyLink(tvr, newOjvProperty, filter, tvr.getTvrType());

    builder.transform();
  }

  private RelNode copyWithNewInputType(LogicalFilter filter, RelNode child,
      RelOptCluster cluster, RelOptRuleCall call) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataType childRowType = child.getRowType();
    RelOptUtil.RexInputConverter converter =
        new RelOptUtil.RexInputConverter(rexBuilder, null,
            childRowType.getFieldList(), new int[childRowType.getFieldCount()]);
    RelNode newRel = filter.copy(filter.getTraitSet(), child,
        filter.getCondition().accept(converter));
    return matchNullable(newRel, filter.getRowType(), call.builder());
  }
}
