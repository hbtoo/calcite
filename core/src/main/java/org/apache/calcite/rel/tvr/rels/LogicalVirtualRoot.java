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
package org.apache.calcite.rel.tvr.rels;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import java.util.List;

public class LogicalVirtualRoot extends VirtualRoot {

  protected LogicalVirtualRoot(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs) {
    super(cluster, traitSet, inputs);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new LogicalVirtualRoot(getCluster(), traitSet, inputs);
  }

  /**
   * Create a virtual root based on given inputs. If there is only 1 input, return the input
   * itself.
   *
   * @param inputs inputs
   * @return virtual root, or input if given single input.
   */
  public static RelNode create(List<RelNode> inputs) {
    if (inputs == null || inputs.isEmpty()) {
      throw new UnsupportedOperationException("Input of virtual root is null or empty.");
    }
    if (inputs.size() == 1) {
      return inputs.get(0);
    }
    RelNode root = inputs.get(0);
    RelOptCluster cluster = root.getCluster();
    return new LogicalVirtualRoot(cluster, cluster.traitSet(), inputs);
  }

}
