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
package org.apache.calcite.rel.tvr.trait.transformer;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.tvr.trait.transformer.predicate.TransformerPredicate;
import org.apache.calcite.rel.tvr.utils.TvrJsonUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TvrCompositeTransformer extends TvrSemanticsTransformer {

  private final List<TvrSemanticsTransformer> innerTransformers;

  public TvrCompositeTransformer(
      List<TvrSemanticsTransformer> innerTransformers) {
    this.innerTransformers = innerTransformers;
  }

  @Override public Multimap<ImmutableIntList, Integer> forwardMapping() {
    Multimap<ImmutableIntList, Integer> ret = HashMultimap.create();

    for (ImmutableIntList origCols : innerTransformers.get(0).forwardMapping()
        .keySet()) {
      List<ImmutableIntList> curIndices = new ArrayList();
      curIndices.add(origCols);

      for (TvrSemanticsTransformer trans : innerTransformers) {
        List<ImmutableIntList> newIndices = new ArrayList();
        for (ImmutableIntList idx : curIndices) {
          newIndices.addAll(
              trans.forwardMapping().get(idx).stream().map(ImmutableIntList::of)
                  .collect(Collectors.toSet()));
        }
        curIndices = newIndices;
        if (curIndices.isEmpty()) {
          break;
        }
      }
      for (ImmutableIntList mappedCols : curIndices) {
        mappedCols.forEach(i -> ret.put(origCols, i));
      }
    }
    return ret;
  }

  @Override public Map<Integer, ImmutableIntList> backwardMapping() {
    Map<Integer, ImmutableIntList> ret = new HashMap<>();

    for (Integer transformedCols : innerTransformers
        .get(innerTransformers.size() - 1).backwardMapping().keySet()) {
      List<Integer> curIndices = new ArrayList();
      curIndices.add(transformedCols);

      for (int i = innerTransformers.size() - 1; i >= 0; i--) {
        TvrSemanticsTransformer trans = innerTransformers.get(i);
        List<ImmutableIntList> newIndices = new ArrayList();
        for (int idx : curIndices) {
          newIndices.add(trans.backwardMapping().get(idx));
        }
        curIndices =
            newIndices.stream().flatMap(ImmutableIntList::stream).distinct()
                .collect(Collectors.toList());
        if (curIndices.isEmpty()) {
          break;
        }
      }
      ret.put(transformedCols, ImmutableIntList.copyOf(curIndices));
    }
    return ret;
  }

  @Override public RelDataType deriveRowType(RelDataType inputRowType,
      RelDataTypeFactory factory) {
    // the row type is the output of final transformer
    for (TvrSemanticsTransformer transformer : innerTransformers) {
      inputRowType = transformer.deriveRowType(inputRowType, factory);
    }
    return inputRowType;
  }

  @Override public TvrSemanticsTransformer addNewTransformer(
      TvrSemanticsTransformer newTransformer) {
    List<TvrSemanticsTransformer> newInnerTrans = new ArrayList<>(
        innerTransformers.subList(0, innerTransformers.size() - 1));
    TvrSemanticsTransformer lastTrans =
        innerTransformers.get(innerTransformers.size() - 1);
    TvrSemanticsTransformer trans = lastTrans.addNewTransformer(newTransformer);
    if (trans instanceof TvrCompositeTransformer) {
      newInnerTrans.addAll(((TvrCompositeTransformer) trans).innerTransformers);
    } else {
      newInnerTrans.add(trans);
    }
    return new TvrCompositeTransformer(newInnerTrans);
  }

  @Override public boolean isCompatible(TransformerPredicate predicate, ImmutableIntList indices) {
    for (int i = innerTransformers.size() - 1; i >= 0; i--) {
      TvrSemanticsTransformer trans = innerTransformers.get(i);

      Map<Integer, ImmutableIntList> backwardMapping = trans.backwardMapping();

      indices = ImmutableIntList.copyOf(indices.stream()
          .flatMap(index -> backwardMapping.get(index).stream())
          .distinct().collect(Collectors.toList()));

      if (!trans.isCompatible(predicate, indices)) {
        return false;
      }

    }
    return true;
  }

  @Override public boolean referToPartialResult(int index) {
    ImmutableIntList newKeys = ImmutableIntList.of(index);
    for (int i = innerTransformers.size() - 1; i >= 0; i--) {
      TvrSemanticsTransformer trans = innerTransformers.get(i);
      if (newKeys.stream().anyMatch(trans::referToPartialResult)) {
        return true;
      }

      Map<Integer, ImmutableIntList> backwardMapping = trans.backwardMapping();
      newKeys = ImmutableIntList.copyOf(
          newKeys.stream().flatMap(j -> backwardMapping.get(j).stream())
              .collect(Collectors.toList()));
    }
    return false;
  }

  @Override public TvrSemanticsTransformer transform(Multimap<Integer, Integer> mapping) {
    List<TvrSemanticsTransformer> newTransforms = new ArrayList();
    for (TvrSemanticsTransformer trans : innerTransformers) {
      TvrSemanticsTransformer newTrans = trans.transform(mapping);
      newTransforms.add(newTrans);
      mapping = creatingNewMapping(trans, newTrans, mapping);
    }
    return new TvrCompositeTransformer(newTransforms);
  }

  @Override public Set<Integer> getRequiredColumns() {
    Set<Integer> keySet = new HashSet<>();
    for (int i = innerTransformers.size() - 1; i >= 0; i--) {
      TvrSemanticsTransformer trans = innerTransformers.get(i);
      Map<Integer, ImmutableIntList> backwardMapping =
          trans.backwardMapping();
      keySet = keySet.stream().flatMap(key -> backwardMapping.get(key).stream())
          .collect(Collectors.toSet());

      keySet.addAll(trans.getRequiredColumns());
    }

    return keySet;
  }

  @Override public List<RelNode> apply(List<RelNode> inputs) {
    List<RelNode> outputs = new ArrayList<>(inputs);
    for (TvrSemanticsTransformer trans : innerTransformers) {
      outputs = trans.apply(outputs);
    }

    return outputs;
  }

  @Override public List<RelNode> consolidate(List<RelNode> inputs) {
    // apply the first transformer
    TvrSemanticsTransformer firstTrans = innerTransformers.get(0);
    assert !(firstTrans instanceof TvrProjectTransformer);
    return  firstTrans.consolidate(inputs);
  }

  private Multimap<Integer, Integer> creatingNewMapping(
      TvrSemanticsTransformer oldTrans, TvrSemanticsTransformer newTrans,
      Multimap<Integer, Integer> mapping) {
    Multimap<Integer, Integer> ret = ArrayListMultimap.create();

    Map<Integer, ImmutableIntList> backwardMapping =
        oldTrans.backwardMapping();
    Multimap<ImmutableIntList, Integer> forwardMapping =
        newTrans.forwardMapping();

    for (Integer origCols : backwardMapping.keySet()) {
      // backward transform
      ImmutableIntList tvrAnyInputCols = backwardMapping.get(origCols);

      // apply mapping
      ImmutableIntList tvrAnyCols = ImmutableIntList.copyOf(
          tvrAnyInputCols.stream().flatMap(i -> mapping.get(i).stream())
              .distinct().sorted().collect(Collectors.toList()));

      // forward transform
      Collection<Integer> tvrCols = forwardMapping.get(tvrAnyCols);

      // save results
      for (Integer mappedCols : tvrCols) {
        ret.put(origCols, mappedCols);
      }
    }

    // the new columns added by the front transformer
    List<Map.Entry<Integer, Integer>> newCols =
        mapping.entries().stream().filter(e -> e.getKey() < 0)
            .collect(Collectors.toList());
    newCols.forEach(e -> {
      Collection<Integer> collection =
          forwardMapping.get(ImmutableIntList.of(e.getValue()));
      collection.forEach(i -> ret.put(e.getKey(), i));
    });

    return ret;
  }

  public List<TvrSemanticsTransformer> getInnerTransformers() {
    return innerTransformers;
  }

  @Override public boolean anyMatch(Predicate<TvrSemanticsTransformer> predicate) {
    return super.anyMatch(predicate) || innerTransformers.stream().anyMatch(predicate);
  }

  @Override public boolean allMatch(Predicate<TvrSemanticsTransformer> predicate) {
    return super.allMatch(predicate) && innerTransformers.stream().allMatch(predicate);
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TvrCompositeTransformer)) {
      return false;
    }
    TvrCompositeTransformer that = (TvrCompositeTransformer) o;
    return Objects.equals(innerTransformers, that.innerTransformers);
  }

  @Override public int hashCode() {
    return Objects.hash(innerTransformers);
  }

  public static class TvrCompositeTransformerSerde
      implements JsonDeserializer<TvrCompositeTransformer>,
      JsonSerializer<TvrCompositeTransformer> {

    protected RelDataType rowType;
    protected RelDataTypeFactory typeFactory;

    public TvrCompositeTransformerSerde(RelDataType rowType,
        RelDataTypeFactory typeFactory) {
      this.rowType = rowType;
      this.typeFactory = typeFactory;
    }

    @Override public TvrCompositeTransformer deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      List<TvrSemanticsTransformer> transformers = new ArrayList<>();
      RelDataType inputRowType = this.rowType;
      for (JsonElement je : jsonObject.getAsJsonArray("innerTransformers")) {
        Gson gson = TvrJsonUtils.createTvrGson(inputRowType, typeFactory);
        TvrSemanticsTransformer transformer =
            gson.fromJson(je.getAsString(), TvrSemanticsTransformer.class);
        inputRowType = transformer.deriveRowType(inputRowType, typeFactory);
        transformers.add(transformer);
      }
      return new TvrCompositeTransformer(transformers);
    }

    @Override public JsonElement serialize(TvrCompositeTransformer src, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();

      JsonArray array = new JsonArray();
      RelDataType inputRowType = this.rowType;
      for (TvrSemanticsTransformer trans : src.innerTransformers) {
        Gson gson = TvrJsonUtils.createTvrGson(inputRowType, typeFactory);
        array.add(gson.toJson(trans, TvrSemanticsTransformer.class));
        inputRowType = trans.deriveRowType(inputRowType, typeFactory);
      }
      jsonObject.add("innerTransformers", array);
      return jsonObject;
    }
  }
}
