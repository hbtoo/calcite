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
package org.apache.calcite.rel.tvr.rules.operators;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTvrRule;
import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.trait.TvrValueDelta;
import org.apache.calcite.rel.tvr.trait.transformer.TvrAggregateTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrProjectTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSemanticsTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSortTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.predicate.TransformerPredicate;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 *
 *
 * <p>Matches:
 * <p>
 *
 *      LogicalSort  --SET_SNAPSHOT_MAX-- Tvr0
 *           |                                         (*Delta)
 *         input     --SET_SNAPSHOT_MAX-- Tvr1 ------------------------     input
 *
 *
 *
 * <p>For TvrSetSemantics it converts to:
 * <p>
 *
 *                                                    (ValueDelta)
 *      LogicalSort    --SET_SNAPSHOT_MAX-- Tvr0 ------------------------   newLogicalSort
 *           |                                         (SetDelta)                 |
 *         input       --SET_SNAPSHOT_MAX-- Tvr1 ------------------------       input
 *
 *
 *
 * <p>For TvrValueSemantics it converts to:
 *
 *
 * <p>                                                   (ValueDelta)
 *      LogicalSort    --SET_SNAPSHOT_MAX-- Tvr0 ------------------------   newLogicalSort
 *           |                                        (ValueDelta)               |
 *         input       --SET_SNAPSHOT_MAX-- Tvr1 ------------------------     input
 * </p>
 */
public abstract class TvrSortRules {
  public static final TvrSetSemanticsSortRule TVR_SET_SEMANTICS_SORT_RULE =
      new TvrSetSemanticsSortRule();
  public static final TvrValueSemanticsSortRule TVR_VALUE_SEMANTICS_SORT_RULE =
      new TvrValueSemanticsSortRule();
//  public static final TvrTwoPhaseLimitRule TVR_TWO_PHASE_LIMIT_RULE =
//      new TvrTwoPhaseLimitRule();
//  public static final TvrThreePhaseLimitRule TVR_THREE_PHASE_LIMIT_RULE =
//      new TvrThreePhaseLimitRule();
}

abstract class TvrSortRuleBase extends RelOptTvrRule {
  TvrSortRuleBase(Class<? extends TvrSemantics> tvrClass,
      Predicate<? super TvrSemantics> predicate) {
    super(
        operand(LogicalSort.class,
        tvrEdgeSSMax(tvr()), operand(RelSubset.class,
            tvrEdgeSSMax(tvr(tvrEdge(tvrClass, predicate, logicalSubset()))),
            any())));
  }

  protected BigDecimal getOffset(LogicalSort logicalSort) {
    if (logicalSort.offset == null) {
      return null;
    }
    return (BigDecimal) ((RexLiteral) logicalSort.offset).getValue();
  }
}

class TvrSetSemanticsSortRule extends TvrSortRuleBase {
  TvrSetSemanticsSortRule() {
    super(TvrSetDelta.class, t -> ((TvrSetDelta) t).isPositiveOnly());
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    LogicalSort logicalSort = root.get();
    TvrSemantics inputTrait = root.input(0).tvrSibling().tvrSemantics;
    RelNode input = root.input(0).tvrSibling().rel.get();
    BigDecimal limit = (BigDecimal) ((RexLiteral) logicalSort.fetch).getValue();
    BigDecimal offset = getOffset(logicalSort);

    final RelBuilder relBuilder = call.builder();
    final RexNode newLimit = logicalSort.offset == null
        ? logicalSort.fetch
        : relBuilder.literal(limit.add(offset).longValue());
    RelDistribution distribution =
        logicalSort.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
    RelNode newSort = LogicalSort.create(input, logicalSort.collation, null, newLimit);

    TvrSortTransformer sortTransformer =
        new TvrSortTransformer(null, logicalSort.collation, limit, offset,
            distribution, null, logicalSort.getRowType().getFieldCount());

    TvrValueDelta outputTrait =
        new TvrValueDelta(inputTrait.fromVersion, inputTrait.toVersion,
            sortTransformer, TvrUtils.IS_SNAPSHOT_TIME.test(inputTrait));

    transformToRootTvr(call, newSort, outputTrait);
  }
}

class TvrValueSemanticsSortRule extends TvrSortRuleBase {
  TvrValueSemanticsSortRule() {
    super(TvrValueDelta.class, t -> ((TvrValueDelta) t).isPositiveOnly());
  }

  @Override public boolean matches(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    LogicalSort logicalSort = root.get();

    TvrValueDelta inputTrait =
        (TvrValueDelta) root.input(0).tvrSibling().tvrSemantics;

    // the sort column can not be the result of partial aggregate
    if (null != logicalSort.collation) {

      TransformerPredicate predicate = new TransformerPredicate() {
        @Override public boolean test(TvrAggregateTransformer transformer,
            ImmutableIntList indices) {
          return false;
        }
        @Override public boolean test(TvrProjectTransformer transformer, ImmutableIntList indices) {
          return indices.size() == 1;
        }
        @Override public boolean test(TvrSortTransformer transformer, ImmutableIntList indices) {
          return false;
        }
      };

      return logicalSort.collation.getFieldCollations().stream().allMatch(
          p -> inputTrait.getTransformer()
              .isCompatible(predicate, ImmutableIntList.of(p.getFieldIndex())));
    }
    return true;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelMatch root = getRoot(call);
    LogicalSort logicalSort = root.get();
    RelNode input = root.input(0).tvrSibling().rel.get();
    TvrValueDelta inputTrait =
        (TvrValueDelta) root.input(0).tvrSibling().tvrSemantics;
    TvrSemanticsTransformer transformer = inputTrait.getTransformer();
    Map<Integer, ImmutableIntList> backwardMapping =
        transformer.backwardMapping();

    BigDecimal limit = (BigDecimal) ((RexLiteral) logicalSort.fetch).getValue();
    BigDecimal offset = getOffset(logicalSort);
    RelCollation relCollation = logicalSort.collation;
    if (null != relCollation) {
      List<RelFieldCollation> collations = relCollation.getFieldCollations();
      List<RelFieldCollation> newCollations =
          collations.stream().map(collation -> {
            int index = collation.getFieldIndex();
            return backwardMapping.get(index).stream().map(
                i -> new RelFieldCollation(i, collation.direction,
                    collation.nullDirection)).collect(Collectors.toSet());
          }).flatMap(Collection::stream).collect(Collectors.toList());
      relCollation = new RelCollationImpl(ImmutableList.copyOf(newCollations));
    }

    int targetCount = backwardMapping.keySet().size();
    Mappings.TargetMapping colMapping = Mappings
        .create(MappingType.FUNCTION, input.getRowType().getFieldCount(),
            targetCount);
    backwardMapping.forEach((key, argList) -> {
      // all of them are identity maps
      assert argList.size() == 1;
      colMapping.set(key, argList.get(0));
    });
    RelDistribution distribution =
        logicalSort.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
    RelDistribution newDistribution = distribution.apply(colMapping);

    // init forward mapping and backward mapping, all of them are identity maps
    int inputColNum =
        (int) transformer.forwardMapping().values().stream().distinct().count();
    final RelBuilder relBuilder = call.builder();
    final RexNode newLimit = logicalSort.offset == null
        ? logicalSort.fetch
        : relBuilder.literal(limit.add(offset).longValue());
    RelNode newSort = LogicalSort.create(input, relCollation, null, newLimit);

    TvrSortTransformer sortTransformer =
        new TvrSortTransformer(null, relCollation, limit, offset,
            newDistribution, null, inputColNum);
    TvrSemanticsTransformer newTransformer =
        inputTrait.getTransformer().addNewTransformer(sortTransformer);

    TvrValueDelta newValueTrait =
        new TvrValueDelta(inputTrait.fromVersion, inputTrait.toVersion,
            newTransformer, TvrUtils.IS_SNAPSHOT_TIME.test(inputTrait));

    transformToRootTvr(call, newSort, newValueTrait);
  }
}
