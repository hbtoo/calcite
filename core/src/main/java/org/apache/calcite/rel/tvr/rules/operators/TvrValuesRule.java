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
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.RelSet;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.tvr.TvrVolcanoPlanner;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.VersionInterval;

import java.util.Arrays;

public class TvrValuesRule extends RelOptTvrRule {
  public static final TvrValuesRule INSTANCE = new TvrValuesRule(false, false);
  public static final TvrValuesRule SAMPLE_INSTANCE = new TvrValuesRule(true, false);
  public static final TvrValuesRule COPY_INSTANCE = new TvrValuesRule(false, true);

  private final boolean sample;
  private final boolean copy;

  private TvrValuesRule(boolean sample, boolean copy) {
    super(operand(Values.class, tvrEdgeSSMax(tvr()), none()),
        "TvrValuesRule-" + (sample ? "sample" : "full") + (copy ? "-copy" : ""));
    this.sample = sample;
    this.copy = copy;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    TvrMetaSetType tvrType = root.tvr().get().getTvrType();
    Values values = root.get();
    RelBuilder builder = call.builder();

    VersionInterval[] deltas = tvrType.getDeltas();
    TvrVersion[] snapshots = tvrType.getSnapshots();

    if (sample) {
      deltas = deltas.length == 0 ? null : Arrays.copyOfRange(deltas, 0, 1);
      snapshots = Arrays.copyOfRange(snapshots, 0, 1);
    }

    VersionInterval deltaSeed = null;
    TvrVersion snapshotSeed = null;
    TvrVolcanoPlanner planner = null;
    if (copy) {
      deltaSeed = deltas == null || deltas.length == 0 ? null : deltas[0];
      snapshotSeed = snapshots[0];
      planner = (TvrVolcanoPlanner) call.getPlanner();
    }

    // snapshot is always the same
    for (TvrVersion s : snapshots) {
      transformToRootTvr(call, values, new TvrSetSnapshot(s));
      if (copy && s == snapshotSeed) {
        RelSet seed = planner.getSet(planner.ensureRegistered(values, null));
        planner.copyHelper.seed(seed, tvrType);
      }
    }
    // delta is always empty
    if (deltas == null) {
      return;
    }
    RelNode empty = builder.values(values.getRowType()).build();
    for (VersionInterval d : deltas) {
      transformToRootTvr(call, empty, new TvrSetDelta(d.from, d.to, true));
      if (copy && d.equals(deltaSeed)) {
        RelSet seed = planner.getSet(planner.ensureRegistered(empty, null));
        planner.copyHelper.seed(seed, tvrType);
      }
    }
  }

}
