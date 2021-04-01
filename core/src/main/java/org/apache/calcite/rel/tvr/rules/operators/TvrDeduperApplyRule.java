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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.tvr.rels.LogicalTvrDeduper;

public class TvrDeduperApplyRule extends RelOptRule {

  public static final TvrDeduperApplyRule INSTANCE = new TvrDeduperApplyRule();

  private TvrDeduperApplyRule() {
    super(operand(LogicalTvrDeduper.class, any()),
        "TvrDeduperApplyRule");
  }

  @Override public void onMatch(RelOptRuleCall call) {
    LogicalTvrDeduper deduper = call.rel(0);
    call.transformTo(deduper.apply());
  }
}
