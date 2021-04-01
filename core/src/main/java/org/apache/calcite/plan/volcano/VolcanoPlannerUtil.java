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

import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.utils.ProgressiveMetrics;

import com.google.common.collect.ImmutableMap;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by tedxu on 14/04/2017.
 */
public class VolcanoPlannerUtil {

  private VolcanoPlannerUtil() {
  }

  public static RelNode buildCheapestPlan(VolcanoPlanner planner, RelSubset rel) {
    return rel.buildCheapestPlan(planner);
  }

  public static void printAllJoins(VolcanoPlanner planner) {
    throw new UnsupportedOperationException("printAllJoins unsupported");
  }

  public static void printAllTvrInfo(VolcanoPlanner volcanoPlanner) {
    System.out.println("all tvr info:");

    // add tvr info
    List<TvrMetaSet> tvrs =
        volcanoPlanner.allSets.stream().flatMap(set -> set.tvrLinks.values().stream()).distinct()
            .sorted(Comparator.comparing(t -> t.getTvrId())).collect(Collectors.toList());

    for (TvrMetaSet tvr : tvrs) {
      HashMap<String, Integer> tvrSets = new HashMap<>();
      tvr.tvrSets.forEach((tvrSemantics, set) -> {
        // find the most recent set
        if (set.rel == null) {
          return;
        }
        set = volcanoPlanner.getSet(set.rel);
        tvrSets.put(tvrSemantics.toString(), set.id);
      });

      ImmutableMap.Builder<String, TvrMetaSet> tvrPropertyLinks = ImmutableMap.builder();
      tvr.tvrPropertyLinks.forEach((tvrProperty, toTvr) -> {
        tvrPropertyLinks.put(tvrProperty.toString(), toTvr);
      });

      StringBuilder sb = new StringBuilder();
      sb.append("tvr#").append(tvr.getTvrId()).append(" - ").append(tvr.getTvrType());
      sb.append("\n");

      sb.append("tvr sets: \n");
      for (Map.Entry<TvrSemantics, RelSet> entry : tvr.tvrSets.entrySet()) {
        TvrSemantics tvrSemantics = entry.getKey();
        RelSet set = entry.getValue();
        sb.append("\t");
        sb.append(tvrSemantics.toString()).append(" - ").append("set#").append(set.getId())
            .append("\n");
      }

      sb.append("tvr property links: \n");
      if (tvr.tvrPropertyLinks.isEmpty()) {
        sb.append("\t").append("empty").append("\n");
      }
      for (Map.Entry<TvrProperty, TvrMetaSet> entry : tvr.tvrPropertyLinks.entrySet()) {
        sb.append("\t");
        TvrProperty tvrProperty = entry.getKey();
        TvrMetaSet toTvr = entry.getValue();
        sb.append(tvrProperty.toString()).append(" - ").append("tvr#").append(toTvr.tvrId)
            .append("\n");
      }

      System.out.println(sb.toString());

    }

  }

  public static Map<RelNode, ?> provenance(VolcanoPlanner planner) {
    return planner.provenanceMap;
  }

  public static void recordMemoMetrics(VolcanoPlanner planner, ProgressiveMetrics pm) {
    pm.setRelSetCountTotal(planner.allSets.size());
    planner.allSets.forEach(set -> {
      set.getRelsFromAllSubsets().forEach(rel -> {
        Integer n = pm.relNodeCount.computeIfAbsent(rel.getClass(), x -> new Integer(0));
        pm.relNodeCount.put(rel.getClass(), n + 1);
      });
    });
    pm.setRelNodeCountTotal(
        pm.relNodeCount.entrySet().stream().mapToInt(entry -> entry.getValue()).sum());
  }

  public static String printRelSets(List<RelSet> allSets, Set<Integer> setIds) {
    StringBuilder str = new StringBuilder();
    allSets.stream().filter(set -> setIds.isEmpty() || setIds.contains(set.getId()))
        .forEach(set -> {
          str.append(
              "Set " + set.getId() + ": " + (set.rel == null ? "" : set.rel.toString()) + "\n");

          str.append("  ");
          set.getSubsets().forEach(subset -> str.append(subset.toString() + ", "));
          str.append("\n\n");

          set.tvrLinks.asMap().forEach((tvrKey, tvrMetaSets) -> {
            str.append(
                "  " + (tvrKey.equals(TvrSemantics.SET_SNAPSHOT_MAX) ? "SSMax" : tvrKey.toString())
                    + " --> ");
            tvrMetaSets.forEach(tvr -> str.append("tvr" + tvr.getTvrId() + ", "));
            str.append("\n");
          });
          str.append("\n");
        });
    return str.toString();
  }
}
