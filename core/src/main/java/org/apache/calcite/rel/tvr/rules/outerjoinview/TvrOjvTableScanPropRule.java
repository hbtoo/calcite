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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.TvrMetaSet;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.tvr.trait.property.TvrOuterJoinViewProperty;
import org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;
import org.apache.calcite.rel.tvr.utils.TimeInterval;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrTableUtils;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.IntStream;

public class TvrOjvTableScanPropRule extends RelOptTvrRule
    implements InterTvrRule {

  public static final TvrOjvTableScanPropRule INSTANCE = new TvrOjvTableScanPropRule();

  private TvrOjvTableScanPropRule() {
    super(
        operand(LogicalTableScan.class, r -> !TvrTableUtils.isDerivedTable(r)
            && TvrTableUtils.findTvrRelatedTableMapStr(r) == null, tvrEdgeSSMax(tvr()), none()));
  }

  @Override public boolean matches(RelOptRuleCall call) {
    TvrMetaSet tvr = getRoot(call).tvr().get();
    RelOptCluster cluster = getRoot(call).get().getCluster();
    TvrContext ctx = TvrContext.getInstance(cluster);
    return tvr.getTvrType().equals(ctx.getDefaultTvrType())
        && ctx.allNonDimTablesSorted().size() > 0;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    LogicalTableScan tableScan = getRoot(call).get();
    TvrMetaSet tvr = getRoot(call).tvr().get();
    TvrMetaSetType tvrType = tvr.getTvrType();

    RelOptCluster cluster = tableScan.getCluster();
    TvrContext ctx = TvrContext.getInstance(cluster);
    assert tvr.getTvrType().equals(ctx.getDefaultTvrType());

    // LinkedHashMap are used to preserve register order, especially that
    // TvrOuterJoinViewProperty is registered last
    RelOptRuleCall.TransformBuilder builder = call.transformBuilder();

    RelBuilder relBuilder = call.builder();
    RelOptTable relOptTable = tableScan.getTable();

    ctx.allNonDimTablesSorted().forEach(updateTable -> {
      int tableOrd = ctx.tableOrdinal(updateTable);
      TvrMetaSetType updateOneTableTvrType = TvrPropertyUtil
          .getUpdateOneTableTvrType(tableOrd, tvrType);
      RelNode ts;
      if (TvrUtils.isDimTable(ctx, tableScan)) {
        ts = tableScan;
      } else {
        int ord = ctx.tableOrdinal(tableScan);
        TvrVersion[] snapshots = updateOneTableTvrType.getSnapshots();
        TvrVersion snapshot = snapshots[snapshots.length - 1];
        long scanTo = snapshot.get(ord);
        TimeInterval inv = TimeInterval.of(TvrVersion.MIN_TIME, scanTo);
//        if (ctx.getTvrDataArrivalInfos()
//            .containsKey(relOptTable.getTable().getName())) {
//          ts = TvrTableScanRule.createMockPartitionTableScan(relOptTable, inv,
//              new TvrSetSnapshot(updateOneTableTvrType.getSnapshots()[
//                  updateOneTableTvrType.getSnapshots().length - 1]), relBuilder,
//              cluster).left;
//        } else {
//          ts = TvrTableScanRule
//              .createTableScan(relOptTable, inv, relBuilder, cluster);
//        }
        ts = null;
      }
      builder.addPropertyLink(tvr,
          new TvrUpdateOneTableProperty(tableOrd, TvrUpdateOneTableProperty.PropertyType.OJV), ts,
          updateOneTableTvrType);
    });

    boolean isDimTable =
        TvrUtils.isDimTable(TvrContext.getInstance(cluster), tableScan);

    Integer tableIndex = isDimTable ? null : ctx.tableOrdinal(tableScan);
    LinkedHashSet<Integer> orderedTableSet = new LinkedHashSet<>();
    if (!isDimTable) {
      orderedTableSet.add(tableIndex);
    }

    List<LinkedHashSet<Integer>> terms = ImmutableList.of(orderedTableSet);

    List<LinkedHashSet<Integer>> nonNullTermTables = new ArrayList<>();
    IntStream.range(0, tableScan.getRowType().getFieldCount())
        .forEach(i -> nonNullTermTables.add(orderedTableSet));

    TvrOuterJoinViewProperty newOjvProperty =
        new TvrOuterJoinViewProperty(terms, nonNullTermTables,
            isDimTable ? ImmutableSet.of() : ImmutableSet.of(tableIndex));
    assert TvrPropertyUtil.checkExisitingOJVProperty(call, tvr, tableScan, newOjvProperty)
        : "TvrOuterJoinViewProperty doesn't match at " + tableScan.getId();

    // Add the TvrOuterJoinViewProperty self loop on root tvr
    builder.addPropertyLink(tvr, newOjvProperty, tableScan, tvr.getTvrType());

    builder.transform();
  }
}
