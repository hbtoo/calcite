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
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrUtils;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.calcite.rel.tvr.utils.TvrUtils.EQUIV_SNAPSHOT;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.IS_SNAPSHOT_TIME;
import static org.apache.calcite.rel.tvr.utils.TvrUtils.convertToSetDeltaIfNot;

/**
 * Super SetDelta/SetSnapshot + SetDelta -> Super SetDelta/SetSnapshot
 */
public class TvrSetDeltaMergeRule extends TvrConverterRule {

  public static final TvrSetDeltaMergeRule INSTANCE = new TvrSetDeltaMergeRule();

  private TvrSetDeltaMergeRule() {
    super(ImmutableList.of(TvrSetDelta.class));
  }

  @Override public List<TvrConvertMatch> match(TvrMetaSet tvr, TvrSemantics newTrait,
      RelOptCluster cluster) {
    TvrSetDelta delta = (TvrSetDelta) newTrait;

    if (TvrUtils
        .progressiveBigDeltaEnabled(TvrContext.getInstance(cluster))) {
      // Allow all merge of any two delta/snapshot as long as the time aligns
      return tvr.allTvrSemantics().stream().flatMap(p -> {
        if (p instanceof TvrSetDelta) {
          if (p.toVersion.equals(delta.fromVersion)) {
            return Stream.of(create((TvrSetDelta) p, delta, cluster, tvr));
          } else if (p.fromVersion.equals(delta.toVersion)) {
            return Stream.of(create(delta, (TvrSetDelta) p, cluster, tvr));
          }
        }
        return Stream.empty();
      }).collect(Collectors.toList());
    }

    if (IS_SNAPSHOT_TIME.test(newTrait)) {
      return tvr.allTvrSemantics().stream().filter(
          x -> x instanceof TvrSetDelta && x.fromVersion.equals(newTrait.toVersion))
          .map(d -> create(delta, (TvrSetDelta) d, cluster, tvr))
          .collect(Collectors.toList());
    }

    return tvr.allTvrSemantics().stream().filter(
        p -> p instanceof TvrSetDelta && EQUIV_SNAPSHOT.test(p)
            && p.toVersion.equals(delta.fromVersion))
        .map(p -> create((TvrSetDelta) p, delta, cluster, tvr))
        .collect(Collectors.toList());
  }

  private TvrConvertMatch create(TvrSetDelta leftTrait, TvrSetDelta rightTrait,
      RelOptCluster cluster, TvrMetaSet tvr) {
    boolean isPositive =
        leftTrait.isPositiveOnly() && rightTrait.isPositiveOnly();
    TvrSetDelta outTrait =
        new TvrSetDelta(leftTrait.fromVersion, rightTrait.toVersion,
            isPositive);

    RelNode leftInput = getInputSubset(cluster, tvr, leftTrait);
    RelNode rightInput = getInputSubset(cluster, tvr, rightTrait);
    if (!isPositive) {
      leftInput = convertToSetDeltaIfNot(leftInput, leftTrait);
      rightInput = convertToSetDeltaIfNot(rightInput, rightTrait);
    }
    RelNode rel =
        LogicalUnion.create(Arrays.asList(leftInput, rightInput), true);
    return new TvrConvertMatch(outTrait, ImmutableList.of(rel));
  }
}
