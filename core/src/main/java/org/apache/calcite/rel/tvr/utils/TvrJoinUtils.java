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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ImmutableNullableList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Join related utils.
 */
public class TvrJoinUtils {
  private TvrJoinUtils() {}

  public static RexNode getLogicalJoinCondition(RelNode join) {
    if (join instanceof MultiJoin) {
      return getTwoInputMultiJoinCondition((MultiJoin) join);
    }
    return ((LogicalJoin) join).getCondition();
  }

  public static JoinRelType getLogicalJoinType(RelNode join) {
    if (join instanceof MultiJoin) {
      return getTwoInputMultiJoinType((MultiJoin) join);
    }
    return ((LogicalJoin) join).getJoinType();
  }

  public static RexNode getTwoInputMultiJoinCondition(MultiJoin multiJoin) {
    // The logic is only true for MultiJoin with two inputs
    assert multiJoin.getInputs().size() == 2;

    // Refer to combineOuterJoins() for the join
    // type logic of MultiJoin
    if (multiJoin.isFullOuterJoin()) {
      return multiJoin.getJoinFilter();
    }
    if (multiJoin.getJoinTypes().get(0) == JoinRelType.INNER) {
      switch (multiJoin.getJoinTypes().get(1)) {
      case INNER:
        return multiJoin.getJoinFilter();
      case LEFT:
        return multiJoin.getOuterJoinConditions().get(1);
      default:
        throw new RuntimeException(
            "MultiJoin: not expecting join type " + multiJoin.getJoinTypes().get(1));
      }
    }
    assert multiJoin.getJoinTypes().get(0) == JoinRelType.RIGHT;
    return multiJoin.getOuterJoinConditions().get(0);
  }

  public static JoinRelType getTwoInputMultiJoinType(MultiJoin multiJoin) {
    // The logic is only true for MultiJoin with two inputs
    assert multiJoin.getInputs().size() == 2;

    // Refer to combineOuterJoins() for the join
    // type logic of MultiJoin
    if (multiJoin.isFullOuterJoin()) {
      return JoinRelType.FULL;
    }
    if (multiJoin.getJoinTypes().get(0) == JoinRelType.INNER) {
      switch (multiJoin.getJoinTypes().get(1)) {
      case INNER:
        return JoinRelType.INNER;
      case LEFT:
        return JoinRelType.LEFT;
      default:
        throw new RuntimeException(
            "MultiJoin: not expecting join type " + multiJoin.getJoinTypes().get(1));
      }
    }
    assert multiJoin.getJoinTypes().get(0) == JoinRelType.RIGHT;
    assert multiJoin.getJoinTypes().get(1) == JoinRelType.INNER;
    return JoinRelType.RIGHT;
  }

  public static RelNode createJoin(RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType, boolean notUseCalciteMultiJoin) {
    assert !notUseCalciteMultiJoin;
    return createMultiJoin(left, right, condition, joinType);
  }

  public static RelNode createMultiJoin(RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType) {
    RelOptCluster cluster = left.getCluster();
    RexBuilder builder = cluster.getRexBuilder();
    RelDataType outputRowType = TvrRelOptUtils
        .deriveJoinRowType(left.getRowType(), right.getRowType(), joinType,
            cluster.getTypeFactory(), Collections.emptyList());

    List<RelNode> inputs = ImmutableList.of(left, right);

    List<JoinRelType> joinTypes;
    List<RexNode> outerJoinConditions;
    List<RexNode> joinFilters = new ArrayList();
    switch (joinType) {
    case LEFT:
      joinTypes = ImmutableList.of(JoinRelType.INNER, JoinRelType.LEFT);
      outerJoinConditions = ImmutableNullableList.of(null, condition);
      break;
    case RIGHT:
      joinTypes = ImmutableList.of(JoinRelType.RIGHT, JoinRelType.INNER);
      outerJoinConditions = ImmutableNullableList.of(condition, null);
      break;
    default:  // both FULL and INNER
      joinTypes = ImmutableList.of(JoinRelType.INNER, JoinRelType.INNER);
      outerJoinConditions = ImmutableNullableList.of(null, null);
      joinFilters.add(condition);
    }

    List<Integer> joinFieldRefCounts =
        new ArrayList<>(Collections.nCopies(outputRowType.getFieldCount(), 0));
    RexVisitor visitor = new RexVisitorImpl(true) {
      @Override public Object visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        joinFieldRefCounts.set(index, joinFieldRefCounts.get(index) + 1);
        return super.visitInputRef(inputRef);
      }
    };
    condition.accept(visitor);

    int leftFieldCount = left.getRowType().getFieldCount();
    ImmutableMap<Integer, ImmutableIntList> joinFieldRefCountsMap = ImmutableMap
        .of(0, ImmutableIntList.copyOf(joinFieldRefCounts.subList(0, leftFieldCount)), 1,
            ImmutableIntList
                .copyOf(joinFieldRefCounts.subList(leftFieldCount, outputRowType.getFieldCount())));

    return new MultiJoin(cluster, inputs, RexUtil.composeConjunction(builder, joinFilters, false),
        outputRowType, joinType == JoinRelType.FULL, outerJoinConditions, joinTypes,
        ImmutableNullableList.of(null, null), joinFieldRefCountsMap, null);
  }

  /**
   * We don't have RIGHT_SEMI/ANTI, so use LEFT_SEMI/ANTI instead, swap both inputs
   */
  public static RelNode createRightSemiAntiJoin(RexBuilder rexBuilder, RelNode left, RelNode right,
      RexNode condition, boolean useAnti, boolean padNull, boolean generateLogical) {
    int leftCount = left.getRowType().getFieldCount();
    int rightCount = right.getRowType().getFieldCount();
    RexNode newCondition = condition.accept(new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        if (index < leftCount) {
          return rexBuilder.makeInputRef(inputRef.getType(), rightCount + index);
        } else {
          return rexBuilder.makeInputRef(inputRef.getType(), index - leftCount);
        }
      }
    });
    JoinRelType type = useAnti ? JoinRelType.ANTI : JoinRelType.SEMI;
    RelNode leftSemi = createJoin(right, left, newCondition, type, generateLogical);

    ProjectBuilder projectBuilder = ProjectBuilder.anchor(leftSemi);
    // Pad left fields as null if needed
    if (padNull) {
      left.getRowType().getFieldList().forEach(field -> projectBuilder
          .add(rexBuilder.makeNullLiteral(field.getType()), field.getName()));
    }
    return projectBuilder.addAll(0, rightCount).build();
  }
}
