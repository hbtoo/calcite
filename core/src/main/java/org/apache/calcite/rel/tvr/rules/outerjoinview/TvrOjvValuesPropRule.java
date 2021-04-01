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
package org.apache.calcite.rel.tvr.rules.outerjoinview;

import org.apache.calcite.plan.InterTvrRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.tvr.trait.property.TvrOuterJoinViewProperty;
import org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;
import org.apache.calcite.rel.tvr.utils.TvrContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.IntStream;

public class TvrOjvValuesPropRule extends RelOptTvrRule
    implements InterTvrRule {

  public static final TvrOjvValuesPropRule INSTANCE = new TvrOjvValuesPropRule();

  private TvrOjvValuesPropRule() {
    super(operand(Values.class, tvrEdgeSSMax(tvr()), none()));
  }

  @Override public boolean matches(RelOptRuleCall call) {
    TvrMetaSet tvr = getRoot(call).tvr().get();
    RelOptCluster cluster = getRoot(call).get().getCluster();
    TvrContext ctx = TvrContext.getInstance(cluster);
    return tvr.getTvrType().equals(ctx.getDefaultTvrType())
        && ctx.allNonDimTablesSorted().size() > 0;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Values values = getRoot(call).get();
    TvrMetaSet tvr = getRoot(call).tvr().get();
    TvrMetaSetType tvrType = tvr.getTvrType();

    RelOptCluster cluster = values.getCluster();
    TvrContext ctx = TvrContext.getInstance(cluster);
    assert tvr.getTvrType().equals(ctx.getDefaultTvrType());

    // LinkedHashMap are used to preserve register order, especially that
    // TvrOuterJoinViewProperty is registered last
    RelOptRuleCall.TransformBuilder builder = call.transformBuilder();

    // Add vdProperty link for all non-dimension tables
    ctx.allNonDimTablesSorted().forEach(table -> {
      int tableOrd = ctx.tableOrdinal(table);
      TvrMetaSetType ojvTvrType =
          TvrPropertyUtil.getUpdateOneTableTvrType(tableOrd, tvrType);
      builder.addPropertyLink(tvr,
          new TvrUpdateOneTableProperty(tableOrd, TvrUpdateOneTableProperty.PropertyType.OJV),
          values, ojvTvrType);
    });

    // A constant table
    LinkedHashSet<Integer> orderedTableSet = new LinkedHashSet<>();
    List<LinkedHashSet<Integer>> nonNullTermTables = new ArrayList<>();
    IntStream.range(0, values.getRowType().getFieldCount())
        .forEach(i -> nonNullTermTables.add(orderedTableSet));

    TvrOuterJoinViewProperty newOjvProperty =
        new TvrOuterJoinViewProperty(ImmutableList.of(), nonNullTermTables, ImmutableSet.of());
    assert TvrPropertyUtil.checkExisitingOJVProperty(call, tvr, values, newOjvProperty)
        : "TvrOuterJoinViewProperty doesn't match at " + values.getId();

    // Add the TvrOuterJoinViewProperty self loop on root tvr
    builder.addPropertyLink(tvr, newOjvProperty, values, tvr.getTvrType());

    builder.transform();
  }
}
