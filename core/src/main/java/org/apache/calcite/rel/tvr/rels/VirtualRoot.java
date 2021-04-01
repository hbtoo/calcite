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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.Lists;

import java.util.List;

public class VirtualRoot extends AbstractRelNode {
  protected final List<RelNode> inputs;

  /**
   * Creates an <code>VirtualRoot</code>.
   */
  protected VirtualRoot(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs) {
    super(cluster, traitSet);
    this.inputs = Lists.newArrayList(inputs);
  }

  @Override public List<RelNode> getInputs() {
    return inputs;
  }

  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      pw.input("input#" + ord.i, ord.e);
    }
    return pw;
  }

  @Override public void replaceInput(int ordinalInParent, RelNode p) {
    inputs.set(ordinalInParent, p);
  }

  @Override protected RelDataType deriveRowType() {
    return inputs.get(0).getRowType();
  }
}
