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

import org.apache.calcite.adapter.enumerable.EnumerableUnion;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.tvr.TvrVolcanoPlanner;
import org.apache.calcite.rel.tvr.rels.TvrExecPlansOp;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ProgrExecPlanIterator {
  private static final Log LOG = LogFactory.getLog(ProgrExecPlanIterator.class);

  private List<RelNode> planList;
  private int currentIteration;

  private ReservedTransformer reservedTransformer;

  private int regenerateCnt;

  public ProgrExecPlanIterator(RelNode cheapest, ReservedTransformer reservedTransformer) {
    this.reservedTransformer = reservedTransformer;
    Objects.requireNonNull(cheapest);
    regenerateCnt = 0;
    generateIterator(cheapest);
  }

  public void regenerateIterator(long time) {
    RelNode rootNode = null;
    regenerateCnt++;
    if (reservedTransformer != null) {
      if (reservedTransformer.tvrVolcanoPlanner != null) {
        rootNode = reservedTransformer.tvrVolcanoPlanner.reoptimize(time);
      }
//      if (reservedTransformer.hepTransformer != null) {
//        rootNode = reservedTransformer.hepTransformer.transform(rootNode);
//      }
    }
    generateIterator(rootNode);
  }

  public int getSeqId() {
    return regenerateCnt * 100 + currentIteration - 1;
  }

  private void generateIterator(RelNode cheapest) {
    TvrContext ctx = TvrContext.getInstance(cheapest.getCluster());
    if (!TvrUtils.progressiveEnabled(ctx)) {
      this.planList = Collections.singletonList(cheapest);
    } else {
      // find the operator tree at each time point
      List<RelNode> operatorTreeList = new ArrayList<>();
      if (cheapest instanceof TvrExecPlansOp) {
        for (RelNode input : cheapest.getInputs()) {
          assert input instanceof TvrExecPlansOp;
          // add the roots at each time to the operator tree list
          operatorTreeList.addAll(input.getInputs());
        }
      } else {
        operatorTreeList.add(cheapest);
      }

      this.planList = operatorTreeList;

    }
    this.currentIteration = 0;
  }

  public boolean hasNext() {
    return this.currentIteration < planList.size();
  }

  public RelNode next() {
    int i = currentIteration;
    this.currentIteration++;
    return this.planList.get(i);
  }

  public int getSize() {
    return this.planList.size();
  }

  public static class ReservedTransformer {
    public TvrVolcanoPlanner tvrVolcanoPlanner;
    public HepPlanner hepPlanner;

    public ReservedTransformer(TvrVolcanoPlanner tvrVolcanoPlanner, HepPlanner hepPlanner) {
      this.tvrVolcanoPlanner = tvrVolcanoPlanner;
      this.hepPlanner = hepPlanner;
    }
  }

  /**
   * Merge adjacent unions in the given plan.
   *        C                          C
   *        |                          |
   *      Union    D                 Union   D
   *      /   \   /                  / | \  /
   *   Union  Union      ======>    A  B Union
   *   /  \    / \                        / \
   *  A   B   E  F                       E  F
   */
  private class UnionReducer {
    // <Union, parents of this union>
    // Only these unions that have only one parent can be merged.
    private final Map<RelNode, Set<RelNode>> parents = new HashMap<>();

    private final Map<RelNode, List<RelNode>> visited = new HashMap<>();

    private RelVisitor visitor = new RelVisitor() {
      @Override public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof EnumerableUnion) {
          Set<RelNode> s = parents.computeIfAbsent(node, k -> new HashSet<>());
          s.add(parent);
        }
        super.visit(node, ordinal, parent);
      }
    };

    private List<RelNode> replace(RelNode node, RelNode parent) {
      if (visited.containsKey(node)) {
        return visited.get(node);
      }

      List<RelNode> oldInputs = node.getInputs();
      List<RelNode> newInputs = new ArrayList<>();
      for (RelNode oldInput : oldInputs) {
        List<RelNode> inputs = replace(oldInput, node);
        newInputs.addAll(inputs);
      }

      List<RelNode> result;
      if (node instanceof EnumerableUnion && parent instanceof EnumerableUnion
          && Objects.equals(node.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE),
              node.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE))
          && (((EnumerableUnion) node).all == ((EnumerableUnion) parent).all)
          && parents.get(node).size() == 1) {
        result = newInputs;
      } else {
        result = ImmutableList.of(node.copy(node.getTraitSet(), newInputs));
      }

      visited.put(node, result);
      return result;
    }

    public RelNode transformer(RelNode rel) {
      visitor.go(rel);
      List<RelNode> nodes = replace(rel, null);
      if (nodes.size() != 1) {
        throw new RuntimeException("An unknown error has occurred when merging adjacent unions");
      }
      return nodes.get(0);
    }
  }
}
