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

import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

public class VolcanoRuleMatchLogger implements RelOptListener {
  private final ArrayList<String> log = new ArrayList<>();
  @Override public void relEquivalenceFound(RelEquivalenceEvent event) {
  }

  @Override public void ruleAttempted(RuleAttemptedEvent event) {
  }

  @Override public void ruleProductionSucceeded(RuleProductionEvent event) {
    if (event.isBefore()) {
      return;
    }

    RelOptRuleCall ruleCall = event.getRuleCall();
    StringBuilder builder = new StringBuilder();
    builder.append(ruleCall.getRule().toString());
    builder.append(" [");
    for (RelNode rel : ruleCall.rels) {
      builder.append(rel.getId());
      builder.append(",");
    }
    builder.append("]");
    log.add(builder.toString());
  }

  @Override public void relDiscarded(RelDiscardedEvent event) {
  }

  @Override public void relChosen(RelChosenEvent event) {
  }

  public List<String> getLog() {
    return log;
  }
}
