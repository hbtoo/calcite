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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.rels.LogicalTvrVirtualSpool;
import org.apache.calcite.rel.tvr.rels.TvrVirtualSpool;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.trait.TvrValueDelta;

import static java.util.Collections.singletonList;

public abstract class TvrVirtualSpoolRules {

  public static final TvrSetDeltaTvrVirtualSpoolRule TVR_SET_DELTA_TVR_VIRTUAL_SPOOL_RULE =
      new TvrSetDeltaTvrVirtualSpoolRule();
  public static final TvrValueSemanticsTvrVirtualSpoolRule
      TVR_VALUE_SEMANTICS_TVR_VIRTUAL_SPOOL_RULE = new TvrValueSemanticsTvrVirtualSpoolRule();
}

abstract class TvrTvrVirtualSpoolRuleBase extends RelOptTvrRule {
  TvrTvrVirtualSpoolRuleBase(Class<? extends TvrSemantics> tvrClass) {
    super(
        operand(TvrVirtualSpool.class, tvrEdgeSSMax(tvr()),
        operand(RelSubset.class,
            tvrEdgeSSMax(tvr(tvrEdge(tvrClass, logicalSubset()))), any())));
  }
}

/**
 * Tvr set delta TvrVirtualSpool rule
 */
class TvrSetDeltaTvrVirtualSpoolRule extends TvrTvrVirtualSpoolRuleBase {
  TvrSetDeltaTvrVirtualSpoolRule() {
    super(TvrSetDelta.class);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelMatch tvrVirtualSpoolAny = getRoot(call);
    TvrVirtualSpool logicalTvrVirtualSpool = tvrVirtualSpoolAny.get();
    RelOfTvr input = tvrVirtualSpoolAny.input(0).tvrSibling();

    RelNode newLogicalTvrVirtualSpool = logicalTvrVirtualSpool
        .copy(logicalTvrVirtualSpool.getTraitSet(), singletonList(input.rel.get()));

    transformToRootTvr(call, newLogicalTvrVirtualSpool, input.tvrSemantics);
  }
}

/**
 * Tvr value semantics TvrVirtualSpool rule
 */
class TvrValueSemanticsTvrVirtualSpoolRule extends TvrTvrVirtualSpoolRuleBase {
  TvrValueSemanticsTvrVirtualSpoolRule() {
    super(TvrValueDelta.class);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelMatch tvrVirtualSpoolAny = getRoot(call);
    LogicalTvrVirtualSpool logicalTvrVirtualSpool = tvrVirtualSpoolAny.get();
    RelOfTvr input = tvrVirtualSpoolAny.input(0).tvrSibling();

    RelNode newLogicalTvrVirtualSpool = logicalTvrVirtualSpool
        .copy(logicalTvrVirtualSpool.getTraitSet(), singletonList(input.rel.get()));

    transformToRootTvr(call, newLogicalTvrVirtualSpool, input.tvrSemantics);
  }

}
