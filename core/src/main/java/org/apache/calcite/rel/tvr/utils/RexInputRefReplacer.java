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
package org.apache.calcite.rel.tvr.utils;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.function.IntUnaryOperator;

public class RexInputRefReplacer extends RexShuttle {

  private final RexBuilder builder;
  private IntUnaryOperator mapper;

  public RexInputRefReplacer(RexBuilder builder, IntUnaryOperator mapper) {
    this.builder = builder;
    this.mapper = mapper;
  }

  @Override public RexNode visitInputRef(RexInputRef inputRef) {
    return builder.makeInputRef(inputRef.getType(),
        mapper.applyAsInt(inputRef.getIndex()));
  }
}
