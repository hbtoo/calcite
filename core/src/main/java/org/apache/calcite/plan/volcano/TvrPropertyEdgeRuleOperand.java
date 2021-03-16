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

import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;

import com.google.common.collect.ImmutableList;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * The operand that matches a Tvr Property edge between two Tvrs.
 */
public class TvrPropertyEdgeRuleOperand extends RelOptRuleOperand {

  Class<? extends TvrProperty> propertyClazz;
  Predicate<TvrProperty> predicate;

  TvrRelOptRuleOperand fromTvrOp;
  TvrRelOptRuleOperand toTvrOp;

  public <R extends TvrProperty> TvrPropertyEdgeRuleOperand(
      Class<R> propertyClazz, Predicate<? super R>  predicate,
      TvrRelOptRuleOperand fromTvrOp, TvrRelOptRuleOperand toTvrOp) {
    // Ensures that no RelNode actually matches us
    super(DummyRelNode.class, null, k -> false,
        RelOptRuleOperandChildPolicy.ANY, ImmutableList.of());
    this.propertyClazz = propertyClazz;
    this.predicate = (Predicate<TvrProperty>) predicate;
    this.fromTvrOp = fromTvrOp;
    this.toTvrOp = Objects.requireNonNull(toTvrOp);
  }

  public Class<? extends TvrProperty> getMatchedPropertyClass() {
    return propertyClazz;
  }

  public boolean matches(TvrProperty property) {
    if (!propertyClazz.isInstance(property)) {
      return false;
    }
    return predicate.test(property);
  }

  public TvrRelOptRuleOperand getFromTvrOp() {
    return this.fromTvrOp;
  }

  public void setFromOp(TvrRelOptRuleOperand fromTvrOp) {
    this.fromTvrOp = fromTvrOp;
  }

  public TvrRelOptRuleOperand getToTvrOp() {
    return this.toTvrOp;
  }

  public void setToTvrOp(TvrRelOptRuleOperand toTvrOp) {
    this.toTvrOp = toTvrOp;
  }

  public Predicate<TvrProperty> getPredicate() {
    return predicate;
  }

  public boolean isFromTvrOp(TvrRelOptRuleOperand tvrOp) {
    return this.fromTvrOp == tvrOp;
  }

  public TvrRelOptRuleOperand theOtherEndTvrOp(TvrRelOptRuleOperand tvrOp) {
    if (this.fromTvrOp == tvrOp) {
      return this.toTvrOp;
    } else if (this.toTvrOp == tvrOp) {
      return this.fromTvrOp;
    } else {
      throw new RuntimeException(
          tvrOp + " is not either end of this edge " + this);
    }
  }
}
