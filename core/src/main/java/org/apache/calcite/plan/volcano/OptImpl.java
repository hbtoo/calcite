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

import org.apache.calcite.plan.RelOptNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.stream.Collectors;

class OptImpl {
  public final TRelOptCost bestCost;
  public final RelNode best;
  public final ImmutableBitSet matOption;

  private OptImpl(TRelOptCost bestCost, RelNode best, ImmutableBitSet matOption) {
    this.bestCost = bestCost;
    this.best = best;
    this.matOption = matOption;
  }

  public static OptImpl of(TRelOptCost bestCost, RelNode best, ImmutableBitSet matOption) {
    return new OptImpl(bestCost, best, matOption);
  }

  public boolean inputMaterialized(int i) {
    return matOption.get(i);
  }

  public int time() {
    return bestCost.earliest;
  }

  @Override public String toString() {
    return "<" + best.getId() + best.getInputs().stream().map(RelOptNode::getId)
        .collect(Collectors.toList()) + " : " + matOption + " : " + bestCost
        + ">";
  }
}
