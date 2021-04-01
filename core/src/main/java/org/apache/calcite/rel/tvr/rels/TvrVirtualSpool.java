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

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public abstract class TvrVirtualSpool extends SingleRel {

  protected TvrVirtualSpool(RelTraitSet traits, RelNode input) {
    super(input.getCluster(), traits, input);
  }

  @Override protected RelDataType deriveRowType() {
    return input.getRowType();
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    return mq.getRowCount(input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 1;
    return copy(traitSet, inputs.get(0));
  }

  public abstract RelNode copy(RelTraitSet traitSet, RelNode input);

}
