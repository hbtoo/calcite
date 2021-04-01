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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.tvr.trait.property.TvrOuterJoinViewProperty;
import org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil.updateOneTableProperties;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.matchNullable;


public class TvrOjvProjectPropRule extends RelOptTvrRule
    implements InterTvrRule {

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  public static final TvrOjvProjectPropRule INSTANCE = new TvrOjvProjectPropRule();

  private static RelOptRuleOperand getOp() {
    // A tvr with a tvr property self loop
    TvrPropertyEdgeRuleOperand propertyEdge =
        tvrProperty(TvrOuterJoinViewProperty.class, tvr());
    TvrRelOptRuleOperand tvr = tvr(propertyEdge);
    propertyEdge.setToTvrOp(tvr);

    return operand(LogicalProject.class, tvrEdgeSSMax(tvr()),
        operand(RelSubset.class, tvrEdgeSSMax(tvr), any()));
  }

  private TvrOjvProjectPropRule() {
    super(getOp());
  }

  @Override public void onMatch(RelOptRuleCall call) {
    LogicalProject project = getRoot(call).get();
    TvrMetaSet tvr = getRoot(call).tvr().get();
    TvrMatch childTvr = getRoot(call).input(0).tvr();

    TvrOuterJoinViewProperty childOjvProperty =
        childTvr.property(TvrOuterJoinViewProperty.class);
    Map<TvrUpdateOneTableProperty, TvrMetaSet> childVDProperties =
        updateOneTableProperties(childTvr.get(), TvrUpdateOneTableProperty.PropertyType.OJV);

    RelOptCluster cluster = project.getCluster();
    // LinkedHashMap are used to preserve register order, especially that
    // TvrOuterJoinViewProperty is registered last
    RelOptRuleCall.TransformBuilder builder = call.transformBuilder();

    // Compute all new VD property links
    childVDProperties.forEach((vdProperty, toTvr) -> {
      RelSubset child =
          toTvr.getSubset(TvrSemantics.SET_SNAPSHOT_MAX, cluster.traitSet());
      assert child != null;
      RelNode newRel = copyWithNewInputType(project, child, cluster, call);
      builder.addPropertyLink(tvr, vdProperty, newRel, toTvr.getTvrType());
    });

    List<LinkedHashSet<Integer>> nonNullTermTables = new ArrayList<>();
    Set<Integer> allReferredTables = new HashSet<>();
    for (RexNode field : project.getProjects()) {
      Set<Integer> referredTables = new HashSet<>();
      field.accept(new RexVisitorImpl<Object>(true) {
        @Override public Object visitInputRef(RexInputRef inputRef) {
          Set<Integer> set =
              childOjvProperty.getNonNullTermTables().get(inputRef.getIndex());
          referredTables.addAll(set);
          return null;
        }
      });
      allReferredTables.addAll(referredTables);
      nonNullTermTables
          .add(TvrOuterJoinViewProperty.getOrderedTableSet(referredTables));
    }

    Set<Integer> allChangingTables =
        new HashSet<>(childOjvProperty.allChangingTermTables());
    allChangingTables.removeAll(allReferredTables);
    if (!allChangingTables.isEmpty()) {
      LOGGER.warn("Not supported by ojv algorithm because changing tables "
          + allChangingTables + " got dropped by project " + project);
      return;
    }
    List<LinkedHashSet<Integer>> newTerms = childOjvProperty.getTerms();

    TvrOuterJoinViewProperty newOjvProperty =
        new TvrOuterJoinViewProperty(newTerms, nonNullTermTables,
            childOjvProperty.allChangingTermTables());
    assert TvrPropertyUtil.checkExisitingOJVProperty(call, tvr, project, newOjvProperty)
        : "TvrOuterJoinViewProperty doesn't match at " + project.getId();

    // Add the TvrOuterJoinViewProperty self loop on root tvr
    builder.addPropertyLink(tvr, newOjvProperty, project, tvr.getTvrType());

    builder.transform();
  }

  private RelNode copyWithNewInputType(LogicalProject project,
      RelNode child, RelOptCluster cluster, RelOptRuleCall call) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    RelDataType childRowType = child.getRowType();
    RelOptUtil.RexInputConverter converter =
        new RelOptUtil.RexInputConverter(rexBuilder, null,
            childRowType.getFieldList(), new int[childRowType.getFieldCount()]);

    List<Pair<RexNode, String>> namedProjects = project.getNamedProjects();
    List<RexNode> projects =
        Pair.left(namedProjects).stream().map(expr -> expr.accept(converter))
            .collect(Collectors.toList());
    RelDataType rowType = RexUtil
        .createStructType(typeFactory, projects, Pair.right(namedProjects),
            SqlValidatorUtil.F_SUGGESTER);
    RelNode newRel = project.copy(project.getTraitSet(), child, projects, rowType);
    return matchNullable(newRel, project.getRowType(), call.builder());
  }
}
