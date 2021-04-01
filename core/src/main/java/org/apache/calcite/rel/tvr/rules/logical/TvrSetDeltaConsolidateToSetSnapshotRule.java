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
package org.apache.calcite.rel.tvr.rules.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.TvrConverterRule;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSet.TvrConvertMatch;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrUtils;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

public class TvrSetDeltaConsolidateToSetSnapshotRule extends TvrConverterRule {

  public static final TvrSetDeltaConsolidateToSetSnapshotRule INSTANCE =
      new TvrSetDeltaConsolidateToSetSnapshotRule();

  private TvrSetDeltaConsolidateToSetSnapshotRule() {
    super(ImmutableList.of(TvrSetDelta.class));
  }

  @Override public List<TvrConvertMatch> match(TvrMetaSet tvr,
      TvrSemantics newTrait, RelOptCluster cluster) {
    TvrVersion[] snapshots = tvr.getTvrType().getSnapshots();
    if (!(TvrUtils.IS_SNAPSHOT_TIME.test(newTrait) && newTrait.toVersion
        .equals(snapshots[snapshots.length - 1]))) {
      return Collections.emptyList();
    }

    TvrContext ctx = TvrContext.getInstance(cluster);
    if (!TvrUtils.progressiveConsolidateInAllRelset(ctx)) {
      boolean tvrUnderSink =
          tvr.getRelSet(TvrSemantics.SET_SNAPSHOT_MAX).getSubsets().stream()
              .flatMap(s -> s.getParentRels().stream())
              .filter(s -> s instanceof AbstractConverter).count() == 0;

      if (!tvrUnderSink) {
        return Collections.emptyList();
      }
    }

    TvrSetDelta inputTrait = (TvrSetDelta) newTrait;
    RelNode input = getInputSubset(cluster, tvr, newTrait);
    RelNode rel = inputTrait.isPositiveOnly()
        ? input
        : TvrSetDelta.consolidate(input, input.getCluster().getRexBuilder(), inputTrait, true);

    return ImmutableList
        .of(
            new TvrConvertMatch(new TvrSetSnapshot(newTrait.toVersion),
            ImmutableList.of(rel)));
  }
}
