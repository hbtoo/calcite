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
import org.apache.calcite.plan.volcano.TvrConverterRule;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSet.TvrConvertMatch;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.tvr.trait.TvrValueDelta;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.calcite.rel.tvr.utils.TvrUtils.IS_SNAPSHOT_TIME;

/**
 * This rule is a part of Progressive General Rules. This rule converts trait
 * "TvrValueDelta" (t' - t) to "TvrValueDelta" (MIN - t) by merging a
 * previous super value delta state (MIN - t').
 */
public class TvrValueDeltaMergeRule extends TvrConverterRule {

  public static final TvrValueDeltaMergeRule INSTANCE =
      new TvrValueDeltaMergeRule();

  private TvrValueDeltaMergeRule() {
    super(ImmutableList.of(TvrValueDelta.class));
  }

  private TvrConvertMatch create(TvrValueDelta prev, TvrValueDelta delta,
      RelOptCluster cluster, TvrMetaSet tvr) {
    RelNode leftInput = getInputSubset(cluster, tvr, prev);
    RelNode rightInput = getInputSubset(cluster, tvr, delta);
    RelNode rel =
        LogicalUnion.create(ImmutableList.of(leftInput, rightInput), true);

    // always do the consolidation here to transform the trait to a positive-only value delta
    // FIXME: aggregate transformer will not do the consolidation
    TvrValueDelta to = new TvrValueDelta(prev.fromVersion, delta.toVersion,
        prev.getTransformer(), true);
    return new TvrConvertMatch(to, to.consolidate(ImmutableList.of(rel)));
  }

  @Override public List<TvrConvertMatch> match(TvrMetaSet tvr, TvrSemantics newTrait,
      RelOptCluster cluster) {
    TvrValueDelta delta = (TvrValueDelta) newTrait;

    if (IS_SNAPSHOT_TIME.test(delta)) {
      return tvr.allTvrSemantics().stream().filter(
          d -> d instanceof TvrValueDelta && delta.valueDefEquals(d)
              && d.fromVersion.equals(delta.toVersion))
          .map(d -> create(delta, (TvrValueDelta) d, cluster, tvr))
          .collect(Collectors.toList());
    }

    return tvr.allTvrSemantics().stream().filter(
        p -> p instanceof TvrValueDelta && IS_SNAPSHOT_TIME.test(p) && delta
            .valueDefEquals(p) && p.toVersion.equals(delta.fromVersion))
        .map(p -> create((TvrValueDelta) p, delta, cluster, tvr))
        .collect(Collectors.toList());
  }
}
