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
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrUtils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;

public class VolcanoMemoTimeVisitor {

  private static final int SENTINEL = -1;

  private final VolcanoPlanner planner;
  private Map<RelSubset, Long> visited;

  public VolcanoMemoTimeVisitor(VolcanoPlanner planner) {
    this.planner = planner;
  }

  public Map<RelSubset, Integer> assignTime(RelSubset root) {
    Queue<RelSubset> queue = new ArrayDeque<>();
    visited = new LinkedHashMap<>();
    queue.add(root);
    visited.put(root, tvrTime(root));
    while (!queue.isEmpty()) {
      RelSubset head = queue.poll();
      long headTime = visited.get(head);
      head.getSubsetsSatisfyingThis().forEach(s -> {
        if (s.best == null || s.bestCost.isInfinite() || s == head) {
          return;
        }
        if (!visited.containsKey(s)) {
          queue.add(s);
          visited.put(s, headTime);
        }
      });
      for (RelNode rel : head.getRels()) {
        if (TvrUtils.isOdpsLogicalOperator(rel) || !TvrUtils.notConverter(rel)) {
          continue;
        }
        for (RelNode input : rel.getInputs()) {
          RelSubset s = (RelSubset) input;
          if (s.best == null || s.bestCost.isInfinite()) {
            continue;
          }
          if (!visited.containsKey(s)) {
            queue.add(s);
            long tvrTime = tvrTime(s);
            visited.put(s, tvrTime != SENTINEL ? tvrTime : headTime);
          }
        }
      }
    }

    ArrayList<RelSubset> list = new ArrayList<>(visited.keySet());
    for (int i = list.size() - 1; i >= 0; --i) {
      RelSubset s = list.get(i);
      propagate(s);
    }

    // convert real time to logical time
    long[] instants = TvrContext.getInstance(root.getCluster()).getTvrVersions();
    Map<Long, Integer> mapping = new HashMap<>();
    for (int i = 0; i < instants.length; i++) {
      mapping.put(instants[i], i);
    }

    Map<RelSubset, Integer> ret = new HashMap<>();
    visited.forEach((k, v) -> ret.put(k, mapping.getOrDefault(v, -1)));
    visited = null;
    return ret;
  }

  private long tvrTime(RelSubset subset) {
    if (subset.getSet().hasTvrLink()) {
      long earliest = TvrVersion.MAX_TIME;
      for (TvrSemantics t : subset.getTvrLinks().keySet()) {
        TvrVersion version = t.toVersion;
        if (version.isMax()) {
          continue;
        }
        long latest = version.maxVersion();
        if (earliest > latest) {
          earliest = latest;
        }
      }
      return earliest;
    }
    return SENTINEL;
  }

  private void propagate(RelSubset subset) {
    for (RelNode parent : subset.getParentRels()) {
      if (TvrUtils.isOdpsLogicalOperator(parent) || !TvrUtils
          .notConverter(parent)) {
        continue;
      }

      long local = SENTINEL;
      for (RelNode input : parent.getInputs()) {
        Long t = visited.get(input);
        if (t == null || t == SENTINEL) {
          local = SENTINEL;
          break;
        }
        local = (local == SENTINEL) ? t : Math.max(t, local);
      }

      if (local == SENTINEL) {
        continue;
      }

      long localFinal = local;
      planner.getSubset(parent).getSatisfyingSubsets().forEach(s -> {
        Long t = visited.get(s);
        if (t != null) {
          if (t == SENTINEL || localFinal < t) {
            visited.put(s, localFinal);
            propagate(s);
          }
        }
      });
    }
  }
}
