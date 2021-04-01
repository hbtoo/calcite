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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQN;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQP;

import com.google.common.collect.ImmutableList;

public class TvrStreamingProjectPropRule extends RelOptTvrRule
    implements InterTvrRule {

  public static final TvrStreamingProjectPropRule INSTANCE =
      new TvrStreamingProjectPropRule();

  private TvrStreamingProjectPropRule() {
    super(
        operand(LogicalProject.class, tvrEdgeSSMax(tvr()),
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
   *
   *  LogicalProject  --SET_SNAPSHOT_MAX--  Tvr1
   *         |                                    QP
   *       input      --SET_SNAPSHOT_MAX--  Tvr2 -----> Tvr3  --SET_SNAPSHOT_MAX--  qpInput
   *                                          \   QN
   *                                           --------> Tvr4  --SET_SNAPSHOT_MAX--  qnInput
   *
   * <p>
   * Converts to:
   * <p>
   *                                                 QN
   *  LogicalProject  --SET_SNAPSHOT_MAX--  Tvr1 ---------> Tvr5  --SET_SNAPSHOT_MAX--  newProject1
   *         |                               \   QP                                           |
   *         |                                ------> Tvr6  --SET_SNAPSHOT_MAX-- newProject2  |
   *         |                                                                       |        |
   *         |                                     QP                                |        |
   *       input      --SET_SNAPSHOT_MAX--  Tvr2 -----> Tvr3  --SET_SNAPSHOT_MAX--  qpInput   |
   *                                          \   QN                                          |
   *                                           ---------------> Tvr4  --SET_SNAPSHOT_MAX--  qnInput
   * <p>
   */
  @Override public void onMatch(RelOptRuleCall call) {
    RelMatch match = getRoot(call);
    LogicalProject project = match.get();
    TvrMetaSet rootTvr = match.tvr().get();

    RelNode qnInput =
        match.input(0).tvr().propertyToTvr(TvrStreamingPropertyQN.class)
            .rel(TvrSemantics.SET_SNAPSHOT_MAX).get();
    RelNode qpInput =
        match.input(0).tvr().propertyToTvr(TvrStreamingPropertyQP.class)
            .rel(TvrSemantics.SET_SNAPSHOT_MAX).get();

    RelNode newProject1 =
        project.copy(project.getTraitSet(), ImmutableList.of(qnInput));
    RelNode newProject2 =
        project.copy(project.getTraitSet(), ImmutableList.of(qpInput));

    // QP + QN = SetSnapshotMax
    RelNode merge =
        LogicalUnion.create(ImmutableList.of(newProject1, newProject2), true);

    call.transformBuilder().addEquiv(merge, project)
        .addTvrType(merge, rootTvr.getTvrType())
        .addPropertyLink(rootTvr, TvrStreamingPropertyQP.instance, newProject2,
            rootTvr.getTvrType())
        .addPropertyLink(rootTvr, TvrStreamingPropertyQN.instance, newProject1,
            rootTvr.getTvrType()).transform();
  }
}
