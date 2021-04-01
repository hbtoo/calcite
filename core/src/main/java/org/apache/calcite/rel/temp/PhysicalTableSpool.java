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
package org.apache.calcite.rel.temp;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * table spool operator - stores all input tuples in a table and pass the input to the downstream
 * operator mainly used for materialization in MqcOptimizer
 */
public class PhysicalTableSpool extends PhysicalTableSink implements EnumerableRel {

  public String projName;
  public String tableName;
  public TvrListTable tvrListTable;
  public List rows;
  public RelOptTable relOptTable;

  /**
   * Creates a <code>SingleRel</code>.
   *
   * @param traits
   * @param input  Input relational expression
   */
  public PhysicalTableSpool(RelTraitSet traits, RelNode input, String projName, String tableName) {
    super(traits, input, projName, tableName);
    this.tvrListTable = new TvrListTable(tableName, input.getRowType());
    this.rows = tvrListTable.rows;
    this.projName = projName;
    this.tableName = tableName;

    CalciteCatalogReader schema = TvrUtils.getRootRelOptSchema(getCluster());
    SchemaPlus schemaPlus = schema.getRootSchema().plus();
    schemaPlus.add(tableName, this.tvrListTable);

    this.relOptTable = RelOptTableImpl.create(schema, this.input.getRowType(), this.tvrListTable,
        ImmutableList.of(schemaPlus.getName(), tableName),
        clazz -> Schemas.tableExpression(schemaPlus, Object[].class, tableName, clazz));
  }

  public RelOptTable getTable() {
    return relOptTable;
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    BlockBuilder builder = new BlockBuilder();

    RelNode input = getInput();
    Result inputResult = implementor.visitChild(this, 0, (EnumerableRel) input, pref);

    Expression tableExp = Expressions.convert_(
        Expressions.call(Expressions
            .call(implementor.getRootExpression(),
                BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method),
        BuiltInMethod.SCHEMA_GET_TABLE.method, Expressions.constant(tableName, String.class)),
        ModifiableTable.class);
    Expression collectionExp =
        Expressions.call(tableExp, BuiltInMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION.method);

    Expression inputExp = builder.append("input", inputResult.block);

    Expression spoolExp =
        Expressions.call(BuiltInMethod.TVR_PHYSICAL_TABLE_SPOOL.method, collectionExp, inputExp);
    builder.add(spoolExp);

    PhysType physType = PhysTypeImpl
        .of(implementor.getTypeFactory(), getRowType(), pref.prefer(inputResult.format));
    return implementor.result(physType, builder.toBlock());
  }
}
