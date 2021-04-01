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

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.AbstractRelNode;

import java.util.Comparator;
import java.util.Set;

public class ReuseCandidate {
  public static final Comparator<ReuseCandidate> PLAN_COST_COMPARATOR = (o1, o2) -> {
    RelOptCost c1 = o1.benefit;
    RelOptCost c2 = o2.benefit;
    if (c1.isLt(c2)) {
      return 1;
    }

    if (c2.isLt(c1)) {
      return -1;
    }

    int sum1 = o1.getCandidates().stream().mapToInt(AbstractRelNode::getId).min().orElse(0);
    int sum2 = o2.getCandidates().stream().mapToInt(AbstractRelNode::getId).min().orElse(0);
    return sum1 - sum2;
  };

  // these subsets need to be materialized at the same time
  private Set<RelSubset> candidates;
  // the possible benefits of persisting these subsets
  private RelOptCost benefit;

  public ReuseCandidate(Set<RelSubset> candidates, RelOptCost benefit) {
    this.candidates = candidates;
    this.benefit = benefit;
  }

  public Set<RelSubset> getCandidates() {
    return candidates;
  }

  public RelOptCost getBenefit() {
    return benefit;
  }
}
