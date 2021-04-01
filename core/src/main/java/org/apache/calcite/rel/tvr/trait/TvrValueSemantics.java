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
package org.apache.calcite.rel.tvr.trait;

import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSemanticsTransformer;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.Objects;

public abstract class TvrValueSemantics extends TvrSemantics {

  protected final TvrSemanticsTransformer transformer;

  protected TvrValueSemantics(
      TvrVersion fromVersion, TvrVersion toVersion,
      TvrSemanticsTransformer transformer) {
    super(fromVersion, toVersion);
    this.transformer = Objects.requireNonNull(transformer);
  }

  @Override public RelDataType deriveRowType(RelDataType inputRowType,
      RelDataTypeFactory typeFactory) {
    if (transformer == null) {
      return inputRowType;
    }
    return transformer.deriveRowType(inputRowType, typeFactory);
  }

  public boolean valueDefEquals(Object o) {
    if (!(o instanceof TvrValueSemantics)) {
      return false;
    }
    TvrValueSemantics that = (TvrValueSemantics) o;
    return transformer.equals(that.transformer);
  }

  @Override public boolean equals(Object obj) {
    return obj instanceof TvrValueSemantics && super.equals(obj)
        && valueDefEquals(obj);
  }

  @Override public int hashCode() {
    return Objects.hash(super.hashCode(), transformer);
  }

  public TvrSemanticsTransformer getTransformer() {
    return transformer;
  }
}
