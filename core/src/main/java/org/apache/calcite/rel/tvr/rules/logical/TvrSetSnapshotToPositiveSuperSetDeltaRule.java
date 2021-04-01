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
import org.apache.calcite.plan.volcano.TvrConverterRule;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSet.TvrConvertMatch;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class TvrSetSnapshotToPositiveSuperSetDeltaRule
    extends TvrConverterRule {

  public static final TvrSetSnapshotToPositiveSuperSetDeltaRule INSTANCE =
      new TvrSetSnapshotToPositiveSuperSetDeltaRule();

  private TvrSetSnapshotToPositiveSuperSetDeltaRule() {
    super(ImmutableList.of(TvrSetSnapshot.class));
  }

  @Override public List<TvrConvertMatch> match(TvrMetaSet tvr, TvrSemantics newTrait,
      RelOptCluster cluster) {
    TvrSetDelta setDelta =
        new TvrSetDelta(newTrait.fromVersion, newTrait.toVersion, true);
    RelNode input = getInputSubset(cluster, tvr, newTrait);
    return ImmutableList
        .of(new TvrConvertMatch(setDelta, ImmutableList.of(input)));
  }
}
