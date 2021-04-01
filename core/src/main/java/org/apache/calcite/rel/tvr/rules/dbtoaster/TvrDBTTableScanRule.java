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
package org.apache.calcite.rel.tvr.rules.dbtoaster;

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
import org.apache.calcite.rel.tvr.trait.property.TvrPropertyUtil;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;
import org.apache.calcite.rel.tvr.utils.TimeInterval;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrTableUtils;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.tools.RelBuilder;

public class TvrDBTTableScanRule extends RelOptTvrRule implements InterTvrRule {

  public static final TvrDBTTableScanRule INSTANCE = new TvrDBTTableScanRule();

  private TvrDBTTableScanRule() {
    super(
        operand(LogicalTableScan.class,
            r -> !TvrTableUtils.isDerivedTable(r)
                && TvrTableUtils.findTvrRelatedTableMapStr(r) == null, tvrEdgeSSMax(tvr()),
            none()));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    LogicalTableScan tableScan = getRoot(call).get();
    TvrMetaSet tvr = getRoot(call).tvr().get();
    TvrMetaSetType tvrType = tvr.getTvrType();
    RelOptCluster cluster = tableScan.getCluster();
    TvrContext ctx = TvrContext.getInstance(cluster);

    RelBuilder relBuilder = call.builder();
    RelOptTable relOptTable = tableScan.getTable();

    // Add property link for each changing tables
    for (String updateTable : ctx.allNonDimTablesSorted()) {
      int updateTableOrd = ctx.tableOrdinal(updateTable);
      TvrMetaSetType updateOneTableTvrType =
          TvrPropertyUtil.getDbtUpdateOneTableTvrType(updateTableOrd, tvrType);
      if (updateOneTableTvrType == TvrMetaSetType.DEFAULT) {
        continue;
      }

      RelOptRuleCall.TransformBuilder builder = call.transformBuilder();
      RelNode ts;
      if (TvrUtils.isDimTable(ctx, tableScan)) {
        ts = tableScan;
      } else {
        int ord = ctx.tableOrdinal(tableScan);
        TvrVersion[] snapshots = updateOneTableTvrType.getSnapshots();
        TvrVersion snapshot = snapshots[snapshots.length - 1];
        long scanTo = snapshot.get(ord);
        TimeInterval inv = TimeInterval.of(TvrVersion.MIN_TIME, scanTo);
        ts = null;
      }
      builder.addPropertyLink(
          tvr, new TvrUpdateOneTableProperty(updateTableOrd,
          TvrUpdateOneTableProperty.PropertyType.DB_TOASTER), ts, updateOneTableTvrType);
      builder.transform();
    }
  }
}
