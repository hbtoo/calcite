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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQN;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQP;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrUtils;

public class TvrStreamingValuesPropRule extends RelOptTvrRule
    implements InterTvrRule {

  public static final TvrStreamingValuesPropRule INSTANCE =
      new TvrStreamingValuesPropRule();

  private TvrStreamingValuesPropRule() {
    super(operand(Values.class, tvrEdgeSSMax(tvr()), none()));
  }

  /**
   * Matches:
   * <p>
   *     Values --SET_SNAPSHOT_MAX-- Tvr
   * <p>
   *
   * Converts to:
   * <p>
   *                                   TvrStreamingPropertyQP
   *                                      -------
   *                                    /        \
   *     Values --SET_SNAPSHOT_MAX-- Tvr <-------
   *                                   \
   *                                    -------------> Tvr1 --SET_SNAPSHOT_MAX-- EmptyValues
   *                                    TvrStreamingPropertyQN
   * <p>
   */
  @Override public boolean matches(RelOptRuleCall call) {
    TvrMetaSet tvr = getRoot(call).tvr().get();
    RelOptCluster cluster = getRoot(call).get().getCluster();
    TvrContext ctx = TvrContext.getInstance(cluster);
    return tvr.getTvrType().equals(ctx.getDefaultTvrType());
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelNode rel = getRoot(call).get();
    TvrMetaSet tvr = getRoot(call).tvr().get();

    RelNode emptyValues =
        TvrUtils.makeLogicalEmptyValues(rel.getCluster(), rel.getRowType());

    call.transformBuilder()
        .addPropertyLink(tvr, TvrStreamingPropertyQP.instance, rel,
            tvr.getTvrType())
        .addPropertyLink(tvr, TvrStreamingPropertyQN.instance, emptyValues,
            tvr.getTvrType()).transform();
  }
}
