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

import org.apache.calcite.plan.tvr.TvrSemantics;
import org.apache.calcite.plan.tvr.TvrSetSnapshot;
import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.TvrProperty;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.tvr.trait.TvrSetDelta;
import org.apache.calcite.rel.tvr.trait.TvrSetSnapshotSerde;
import org.apache.calcite.rel.tvr.trait.TvrValueDelta;
import org.apache.calcite.rel.tvr.trait.property.TvrDBToasterCommitProperty;
import org.apache.calcite.rel.tvr.trait.property.TvrOuterJoinViewDoneProperty;
import org.apache.calcite.rel.tvr.trait.property.TvrOuterJoinViewProperty;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQN;
import org.apache.calcite.rel.tvr.trait.property.TvrStreamingPropertyQP;
import org.apache.calcite.rel.tvr.trait.property.TvrUpdateOneTableProperty;
import org.apache.calcite.rel.tvr.trait.transformer.TvrAggregateTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrCompositeTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrProjectTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSemanticsTransformer;
import org.apache.calcite.rel.tvr.trait.transformer.TvrSortTransformer;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.RuntimeTypeAdapterFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class TvrJsonUtils {
  private TvrJsonUtils() {}

  public static Gson createTvrGson(RelDataType rowType, RelDataTypeFactory typeFactory) {
    RuntimeTypeAdapterFactory tvrSemanticsFactory =
        RuntimeTypeAdapterFactory.of(TvrSemantics.class, "className")
            .registerSubtype(TvrSetSnapshot.class).registerSubtype(TvrSetDelta.class)
            .registerSubtype(TvrValueDelta.class);
    RuntimeTypeAdapterFactory tvrPropertiesFactory;
    tvrPropertiesFactory = RuntimeTypeAdapterFactory.of(TvrProperty.class, "className")
        .registerSubtype(TvrStreamingPropertyQP.class).registerSubtype(TvrStreamingPropertyQN.class)
        .registerSubtype(TvrDBToasterCommitProperty.class)
        .registerSubtype(TvrUpdateOneTableProperty.class)
        .registerSubtype(TvrOuterJoinViewProperty.class)
        .registerSubtype(TvrOuterJoinViewDoneProperty.class);
    //    RuntimeTypeAdapterFactory distributionCollationFactory =
    //        RuntimeTypeAdapterFactory.of(RelDistributionBase.class, "className")
    //            .registerSubtype(RelDistributionBase.class)
    //            .registerSubtype(RelHashDistribution.class)
    //            .registerSubtype(RelRangeDistribution.class);
    RuntimeTypeAdapterFactory transformerFactory =
        RuntimeTypeAdapterFactory.of(TvrSemanticsTransformer.class, "className")
            .registerSubtype(TvrProjectTransformer.class)
            .registerSubtype(TvrAggregateTransformer.class)
            .registerSubtype(TvrSortTransformer.class)
            .registerSubtype(TvrCompositeTransformer.class);
    return new GsonBuilder().registerTypeAdapter(TvrSetSnapshot.class, TvrSetSnapshotSerde.instance)
        .registerTypeAdapter(TvrSetDelta.class, TvrSetDelta.TvrSetDeltaSerde.instance)
        //        .registerTypeHierarchyAdapter(RelDataType.class,
        //            new RelDataTypeSerde(typeFactory))
        .registerTypeAdapter(RexLiteral.class, RexLiteralSerde.instance)
        //        .registerTypeAdapter(AggregateCall.class,
        //            new OdpsAggregateCallSerde(rowType))
        .registerTypeAdapter(TvrValueDelta.class,
            new TvrValueDelta.TvrValueDeltaSerde(rowType, typeFactory))
        .registerTypeAdapter(TvrStreamingPropertyQP.class,
            TvrStreamingPropertyQP.TvrStreamingPropertyQPSerde.instance)
        .registerTypeAdapter(TvrStreamingPropertyQN.class,
            TvrStreamingPropertyQN.TvrStreamingPropertyQNSerde.instance)
        .registerTypeAdapter(TvrDBToasterCommitProperty.class,
            TvrDBToasterCommitProperty.TvrDBToasterCommitPropertySerde.instance)
        .registerTypeAdapter(TvrUpdateOneTableProperty.class,
            TvrUpdateOneTableProperty.TvrUpdateOneTablePropertySerde.instance)
        .registerTypeAdapter(TvrOuterJoinViewProperty.class,
            TvrOuterJoinViewProperty.TvrOuterJoinViewPropertySerde.instance)
        .registerTypeAdapter(TvrOuterJoinViewDoneProperty.class,
            TvrOuterJoinViewDoneProperty.TvrOuterJoinViewDonePropertySerde.instance)
        //        .registerTypeAdapter(RelDistributionBase.class,
        //            RelDistributionBaseSerde.INSTANCE)
        //        .registerTypeAdapter(RelHashDistribution.class,
        //            new RelHashDistributionSerde(rowType))
        //        .registerTypeAdapter(RelRangeDistribution.class,
        //            RelRangeDistributionSerde.INSTANCE)
        //        .registerTypeAdapter(OdpsRelFieldCollation.class,
        //            new OdpsRelFieldCollationSerde(rowType))
        //        .registerTypeAdapter(OdpsRelCollationImpl.class,
        //            OdpsRelCollationImplSerde.INSTANCE)
        .registerTypeAdapter(TvrAggregateTransformer.class,
            new TvrAggregateTransformer.TvrAggregateTransformerSerde(rowType, typeFactory))
        .registerTypeAdapter(TvrCompositeTransformer.class,
            new TvrCompositeTransformer.TvrCompositeTransformerSerde(rowType, typeFactory))
        .registerTypeAdapter(TvrProjectTransformer.class,
            new TvrProjectTransformer.TvrProjectTransformerSerde(rowType, typeFactory))
        .registerTypeAdapter(TvrSortTransformer.class,
            TvrSortTransformer.TvrSortTransformerSerde.INSTANCE)
        .registerTypeAdapter(TvrVersion.class, TvrVersionSerde.INSTANCE)
        .registerTypeAdapterFactory(tvrSemanticsFactory)
        .registerTypeAdapterFactory(tvrPropertiesFactory)
        //        .registerTypeAdapterFactory(distributionCollationFactory)
        .registerTypeAdapterFactory(transformerFactory).create();
  }

  public interface ColumnOrderAgnostic {

    default List<Integer> c2i(String[] cols, RelDataType rowType) {
      List<String> fieldNames = rowType.getFieldNames();
      return Arrays.stream(cols).map(fieldNames::indexOf).collect(Collectors.toList());
    }

    default String[] i2c(List<Integer> indices, RelDataType rowType) {
      return indices.stream().map(i -> rowType.getFieldList().get(i).getName())
          .toArray(String[]::new);
    }

  }

  public static class RelDataTypeSerde
      implements JsonDeserializer<RelDataType>, JsonSerializer<RelDataType> {

    private RelDataTypeFactory typeFactory;

    public RelDataTypeSerde(RelDataTypeFactory typeFactory) {
      this.typeFactory = typeFactory;
    }

    @Override public RelDataType deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      String byteString = context.deserialize(json, String.class);
      //        TypeInfoProtos.TypeInfo type =
      //            TypeInfoProtos.TypeInfo.parseFrom(Base64.getDecoder().decode(byteString));
      return convertType(byteString, typeFactory);
    }

    @Override public JsonElement serialize(RelDataType src, Type typeOfSrc,
        JsonSerializationContext context) {
      String type = convertType2(src);
      return context
          .serialize(Base64.getEncoder().encodeToString(type.getBytes(Charset.defaultCharset())));
    }
  }

  public static String convertType2(RelDataType type) {
    throw new UnsupportedOperationException("TvrJsonUtils convertType2 not supported");
  }

  public static RelDataType convertType(String type2, RelDataTypeFactory factory) {
    throw new UnsupportedOperationException("TvrJsonUtils convertType not supported");
  }

  public static class RexLiteralSerde
      implements JsonDeserializer<RexLiteral>, JsonSerializer<RexLiteral> {

    public static RexLiteralSerde instance = new RexLiteralSerde();

    private RexLiteralSerde() {
    }

    @Override public RexLiteral deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      Comparable value = context.deserialize(jsonObject.get("value"), Comparable.class);
      RelDataType type = context.deserialize(jsonObject.get("type"), RelDataType.class);
      SqlTypeName sqlTypeName =
          SqlTypeName.get(context.deserialize(jsonObject.get("sqlTypeName"), String.class));
      return new RexLiteral(value, type, sqlTypeName);
    }

    @Override public JsonElement serialize(RexLiteral src, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      json.add("value", context.serialize(src.getValue()));
      json.add("type", context.serialize(src.getType()));
      json.add("sqlTypeName", context.serialize(src.getTypeName().getName()));
      return json;
    }
  }

  public static RelDataType deserializeRowType(RelDataTypeFactory typeFactory, String rowTypeStr) {
    Gson gson =
        new GsonBuilder().registerTypeAdapter(RelDataType.class, new RelDataTypeSerde(typeFactory))
            .create();
    return gson.fromJson(rowTypeStr, RelDataType.class);
  }

  public static String serializeRowType(RelDataTypeFactory typeFactory, RelDataType rowType) {
    Gson gson =
        new GsonBuilder().registerTypeAdapter(RelDataType.class, new RelDataTypeSerde(typeFactory))
            .create();
    return gson.toJson(rowType, RelDataType.class);
  }

  public static String serializeRelDistributionImpl(RelDistribution distribution) {
    return distribution.getType().shortName;
  }

  public static RelDistribution deserializeRelDistributionImpl(String distributionStr) {
    switch (distributionStr) {
    case "any":
      return RelDistributions.ANY;
    case "single":
      return RelDistributions.SINGLETON;
    case "broadcast":
      return RelDistributions.BROADCAST_DISTRIBUTED;
    case "rr":
      return RelDistributions.ROUND_ROBIN_DISTRIBUTED;
    case "random":
      return RelDistributions.RANDOM_DISTRIBUTED;
    default:
      throw new UnsupportedOperationException();
    }
  }

}

class TvrVersionSerde implements JsonDeserializer<TvrVersion>, JsonSerializer<TvrVersion> {

  public static final TvrVersionSerde INSTANCE = new TvrVersionSerde();

  @Override public TvrVersion deserialize(JsonElement jsonElement, Type type,
      JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = jsonElement.getAsJsonObject();
    long[] versions = context.deserialize(jsonObject.get("version"), long[].class);
    return TvrVersion.of(versions);
  }

  @Override public JsonElement serialize(TvrVersion tvrVersion, Type type,
      JsonSerializationContext context) {
    long[] versions = tvrVersion.getVersions();
    JsonObject jsonObject = new JsonObject();
    jsonObject.add("version", context.serialize(versions));
    return jsonObject;
  }
}
