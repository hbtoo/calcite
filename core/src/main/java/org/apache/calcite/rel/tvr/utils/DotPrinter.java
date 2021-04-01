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
package org.apache.calcite.rel.tvr.utils;

import org.apache.calcite.plan.volcano.RelSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.tools.visualizer.InputExcludedRelWriter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;


public class DotPrinter extends RelVisitor {
  List<String> edges = new ArrayList();
  Set<RelNode> visitedSet = new HashSet();

  private String getId(RelNode node) {
    String id = node.getRelTypeName() + "_" + node.getId();
    return id;
  }

  private String getLabel(RelNode node) {
    StringBuilder label = new StringBuilder();
    label.append("\"");

    label.append(getId(node));

    if (node instanceof TableScan) {
      RelOptTableImpl table = (RelOptTableImpl) node.getTable();
      label.append("  ");
      label.append(table.getQualifiedName().toString());
      TimeInterval interval = table.interval;
      if (interval != null) {
        label.append("@").append(TvrUtils.formatTimeInstant(interval.from))
                .append("-").append(TvrUtils.formatTimeInstant(interval.to));
      }
    }

    label.append("\\n");

    TvrContext ctx = TvrContext.getInstance(node.getCluster());
    if (TvrUtils.progressiveEnabled(ctx) && node.getCluster()
        .getPlanner() instanceof VolcanoPlanner) {
      VolcanoPlanner planner = (VolcanoPlanner) node.getCluster().getPlanner();
      Map<RelNode, RelSubset> mqcBest = ctx.mqcBestPlanMapping;
      if (mqcBest != null) {
        RelSubset subset = mqcBest.get(node);
        RelSet set = subset == null ? null : planner.getSet(subset);
        label.append("subset#");
        label.append(subset == null ? "null" : subset.getId());
        label.append(", ");
        label.append("set#");
        label.append(set == null ? "null" : set.getId());

        String costString = ctx.mqcMemoCostString.get(subset);
        if (costString != null) {
          label.append(" -- " + costString);
        }
      }
    }

    label.append("\\n");

    InputExcludedRelWriter relWriter = new InputExcludedRelWriter();
    node.explain(relWriter);
    label.append(splitIntoLines(relWriter.toString(), 80, 10));

    label.append("\"");

    return label.toString();
  }

  private String splitIntoLines(String str, int maxNumCharsPerLine, int minNumCharsPerLine) {
    StringBuilder sb = new StringBuilder();
    int strLen = str.length();
    for (int i = 0; i < strLen; i++) {
      if (i > 0 && (i % maxNumCharsPerLine == 0) && (strLen - i > minNumCharsPerLine)) {
        sb.append("\\n");
      }
      sb.append(str.charAt(i));
    }
    return sb.toString();
  }

  @Override public void visit(RelNode node, int ordinal, RelNode parent) {
    if (visitedSet.contains(node)) {
      return;
    }
    for (RelNode in : node.getInputs()) {
      String line = String
          .format(Locale.ROOT, "%s -> %s", getId(in), getId(node));
      edges.add(line);
    }
    visitedSet.add(node);
    super.visit(node, ordinal, parent);
  }

  public static String getDot(RelNode node) {
    DotPrinter printer = new DotPrinter();
    printer.go(node);
    StringBuilder sb = new StringBuilder();
    sb.append("digraph {");
    sb.append(System.lineSeparator());
    sb.append("node [fontsize=18];");
    sb.append(System.lineSeparator());
    for (RelNode rel : printer.visitedSet) {
      sb.append(printer.getId(rel));
      sb.append("[" + "label=").append(printer.getLabel(rel)).append("]");
      sb.append(";");
      sb.append(System.lineSeparator());
    }

    for (String l : printer.edges) {
      sb.append(l);
      sb.append(";");
      sb.append(System.lineSeparator());
    }
    sb.append("}");
    sb.append(System.lineSeparator());
    return sb.toString();
  }
}
