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
package org.apache.calcite.rel.tvr.rules.streaming;

import org.apache.calcite.plan.InterTvrRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQN;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQP;
import org.apache.calcite.rel.tvr.utils.TvrJoinUtils;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.stream.IntStream;

import static org.apache.calcite.rel.tvr.utils.TvrJoinUtils.createJoin;
import static org.apache.calcite.rel.tvr.utils.TvrJoinUtils.getLogicalJoinCondition;
import static org.apache.calcite.rel.tvr.utils.TvrJoinUtils.getLogicalJoinType;

public class TvrStreamingJoinPropRule extends RelOptTvrRule
    implements InterTvrRule {

  public static final TvrStreamingJoinPropRule INSTANCE =
      new TvrStreamingJoinPropRule();

  private TvrStreamingJoinPropRule() {
    super(
        operand(MultiJoin.class,
        tvrEdgeSSMax(tvr()),
        operand(
            RelSubset.class, tvrEdgeSSMax(
            tvr(
                tvrProperty(TvrStreamingPropertyQP.class,
                tvr(tvrEdgeSSMax(logicalSubset()))),
                tvrProperty(TvrStreamingPropertyQN.class,
                    tvr(tvrEdgeSSMax(logicalSubset()))))), any()),
        operand(
            RelSubset.class, tvrEdgeSSMax(
            tvr(
                tvrProperty(TvrStreamingPropertyQP.class,
                tvr(tvrEdgeSSMax(logicalSubset()))),
                tvrProperty(TvrStreamingPropertyQN.class,
                    tvr(tvrEdgeSSMax(logicalSubset()))))), any())));
  }

  /**
   * Matches:
   * <p>
   *        Join  --SET_SNAPSHOT_MAX--  Tvr0
   *       /    \
   *      /     \                             QP
   *   LeftInput  --SET_SNAPSHOT_MAX--  Tvr1 -----> Tvr5  --SET_SNAPSHOT_MAX--  qpLeft
   *             \                        \   QN
   *             \                         ------> Tvr6  --SET_SNAPSHOT_MAX--  qnLeft
   *             \
   *             \                                    QP
   *          RightInput  --SET_SNAPSHOT_MAX--  Tvr2 -----> Tvr3  --SET_SNAPSHOT_MAX--  qpRight
   *                                              \  QN
   *                                               -----> Tvr4  --SET_SNAPSHOT_MAX--  qnRight
   * <p>
   */
  @Override public boolean matches(RelOptRuleCall call) {
    RelMatch match = getRoot(call);
    RelNode join = match.get();

    // only supports 2 inputs MultiJoin
    if (join.getInputs().size() != 2) {
      return false;
    }

    JoinRelType joinType = getLogicalJoinType(join);
    switch (joinType) {
    case INNER:
    case LEFT:
    case RIGHT:
      return true;
    default:
      return false;
    }
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelMatch match = getRoot(call);
    RelNode join = match.get();
    TvrMetaSet rootTvr = match.tvr().get();

    RelMatch leftMatch = match.input(0);
    RelMatch rightMatch = match.input(1);
    RelNode left = leftMatch.get();
    RelNode right = rightMatch.get();

    RelNode qpLeft = leftMatch.tvr().propertyToTvr(TvrStreamingPropertyQP.class)
        .rel(TvrSemantics.SET_SNAPSHOT_MAX).get();
    RelNode qnLeft = leftMatch.tvr().propertyToTvr(TvrStreamingPropertyQN.class)
        .rel(TvrSemantics.SET_SNAPSHOT_MAX).get();
    RelNode qpRight =
        rightMatch.tvr().propertyToTvr(TvrStreamingPropertyQP.class)
            .rel(TvrSemantics.SET_SNAPSHOT_MAX).get();
    RelNode qnRight =
        rightMatch.tvr().propertyToTvr(TvrStreamingPropertyQN.class)
            .rel(TvrSemantics.SET_SNAPSHOT_MAX).get();

    RelNode qp;
    RelNode qn;
    RelNode qnPart1;
    RelNode qnPart2;
    RelNode qnPart3;
    JoinRelType joinType = getLogicalJoinType(join);
    RexNode condition = getLogicalJoinCondition(join);

    RexBuilder rexBuilder = left.getCluster().getRexBuilder();
    switch (joinType) {
    case INNER:
      qp = createJoin(qpLeft, qpRight, condition, JoinRelType.INNER, false);

      qnPart1 = createJoin(qnLeft, right, condition, JoinRelType.INNER, false);
      qnPart2 = createJoin(qpLeft, qnRight, condition, JoinRelType.INNER, false);
      qn = LogicalUnion.create(ImmutableList.of(qnPart1, qnPart2), true);
      break;

    case LEFT:
      RelNode qp_raw =
          createJoin(qpLeft, qpRight, condition, JoinRelType.INNER, false);
      // Make sure right side types are set to nullable == true
      qp = call.builder().push(qp_raw).convert(join.getRowType(), true).build();

      qnPart1 = createJoin(qnLeft, right, condition, JoinRelType.INNER, false);
      qnPart2 = createJoin(qpLeft, qnRight, condition, JoinRelType.INNER, false);

      // Create leftAnti join and full right columns with null
      RelNode leftAnti =
          createJoin(left, right, condition, JoinRelType.ANTI, false);

      ArrayList<RexNode> projects = new ArrayList<>();
      ArrayList<String> names = new ArrayList<>();
      IntStream.range(0, leftAnti.getRowType().getFieldList().size())
          .forEach(i -> {
            projects.add(rexBuilder.makeInputRef(leftAnti, i));
            names.add(leftAnti.getRowType().getFieldNames().get(i));
          });
      right.getRowType().getFieldList().forEach(field -> {
        projects.add(rexBuilder.makeNullLiteral(field.getType()));
        names.add(field.getName());
      });
      qnPart3 = LogicalProject.create(leftAnti, ImmutableList.of(), projects, names);

      RelNode qn_raw = LogicalUnion
          .create(ImmutableList.of(qnPart1, qnPart2, qnPart3), true);
      qn = call.builder().push(qn_raw).convert(join.getRowType(), true).build();
      break;

    case RIGHT:
      qp_raw = createJoin(qpLeft, qpRight, condition, JoinRelType.INNER, false);
      // Make sure right side types are set to nullable == true
      qp = call.builder().push(qp_raw).convert(join.getRowType(), true).build();

      qnPart1 = createJoin(qnLeft, right, condition, JoinRelType.INNER, false);
      qnPart2 = createJoin(qpLeft, qnRight, condition, JoinRelType.INNER, false);

      // Create rightAnti join and pad left columns as null
      qnPart3 = TvrJoinUtils
          .createRightSemiAntiJoin(rexBuilder, left, right, condition, true,
              true, false);

      qn_raw = LogicalUnion
          .create(ImmutableList.of(qnPart1, qnPart2, qnPart3), true);
      qn = call.builder().push(qn_raw).convert(join.getRowType(), true).build();
      break;

    default:
      throw new RuntimeException("should not be here for " + joinType);
    }

    // QP + QN = SetSnapshotMax
    RelNode merge = LogicalUnion.create(ImmutableList.of(qp, qn), true);

    call.transformBuilder().addEquiv(merge, join)
        .addTvrType(merge, rootTvr.getTvrType())
        .addPropertyLink(rootTvr, TvrStreamingPropertyQP.instance, qp,
            rootTvr.getTvrType())
        .addPropertyLink(rootTvr, TvrStreamingPropertyQN.instance, qn,
            rootTvr.getTvrType()).transform();
  }

}
