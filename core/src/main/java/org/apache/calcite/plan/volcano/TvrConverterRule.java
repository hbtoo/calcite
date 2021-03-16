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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.tvr.TvrSemantics;

import com.google.common.collect.ImmutableList;

import java.util.List;

public abstract class TvrConverterRule extends RelOptRule implements
    TvrMetaSet.TvrConvertMatchPattern {

  private final List<Class<? extends TvrSemantics>> tvrClasses;

  protected TvrConverterRule(
      ImmutableList<Class<? extends TvrSemantics>> tvrClasses) {
    super(operand(RelOptRuleOperand.DummyRelNode.class, none()));
    this.tvrClasses = tvrClasses;
  }

  @Override public final boolean matches(RelOptRuleCall call) {
    return false;
  }

  @Override public final void onMatch(RelOptRuleCall call) {
  }

  @Override public final List<Class<? extends TvrSemantics>> getRelatedTvrClasses() {
    return tvrClasses;
  }

  protected RelSubset getInputSubset(RelOptCluster cluster, TvrMetaSet tvr,
      TvrSemantics tvrTrait) {
    RelSet set = tvr.getRelSet(tvrTrait);
    return set.getOrCreateSubset(cluster, cluster.traitSet(), true);
  }
}
