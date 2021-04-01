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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.AdhocSink;

public class EnumerablePhysicalAdhocSink extends AdhocSink implements EnumerableRel {

  protected EnumerablePhysicalAdhocSink(RelTraitSet traits, RelNode input) {
    super(traits, input);
  }

  public static EnumerablePhysicalAdhocSink create(RelNode input) {
    return new EnumerablePhysicalAdhocSink(input.getTraitSet(), input);
  }

  public static final RelOptRule ENUMERABLE_PHYSICAL_TVR_ADHOC_SINK_RULE =
      new EnumerablePhysicalAdhocSinkRule();

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // pass through and invoke input
    final BlockBuilder builder = new BlockBuilder();
    final Result result = implementor.visitChild(this, 0, (EnumerableRel) getInput(), pref);
    Expression childExp = builder.append("adhocSinkChild" + 0, result.block);

    builder.add(childExp);
    final PhysType physType = PhysTypeImpl
        .of(implementor.getTypeFactory(), getRowType(), pref.prefer(JavaRowFormat.CUSTOM));
    return implementor.result(physType, builder.toBlock());
  }

  @Override public RelNode copy(RelTraitSet traitSet, RelNode input) {
    return new EnumerablePhysicalAdhocSink(traitSet.replace(EnumerableConvention.INSTANCE), input);
  }

  private static class EnumerablePhysicalAdhocSinkRule extends ConverterRule {
    private EnumerablePhysicalAdhocSinkRule() {
      super(LogicalAdhocSink.class, Convention.NONE, EnumerableConvention.INSTANCE,
          "EnumerablePhysicalAdhocSinkRule");
    }

    @Override public RelNode convert(RelNode rel) {
      RelTraitSet outTraits = rel.getTraitSet().replace(EnumerableConvention.INSTANCE);
      return new EnumerablePhysicalAdhocSink(
          rel.getTraitSet().replace(EnumerableConvention.INSTANCE),
          rel.getCluster().getPlanner().changeTraits(rel.getInput(0), outTraits));
    }
  }
}
