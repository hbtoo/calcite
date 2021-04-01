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
package org.apache.calcite.rel.tvr.trait.property;

import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.TvrProperty;
import org.apache.calcite.util.VersionInterval;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Objects;

/**
 * A done message, meaning TvrOuterJoinViewRule has already fired on the RelNode
 * in this RelSet.
 */
public class TvrOuterJoinViewDoneProperty extends TvrProperty {

  private VersionInterval delta;

  public TvrOuterJoinViewDoneProperty(VersionInterval delta) {
    this.delta = delta;
  }

  @Override public String toString() {
    return "OJVDone:" + delta;
  }

  @Override public int hashCode() {
    return Objects.hashCode(delta);
  }

  @Override public boolean equals(Object other) {
    if (!(other instanceof TvrOuterJoinViewDoneProperty)) {
      return false;
    }
    TvrOuterJoinViewDoneProperty o = (TvrOuterJoinViewDoneProperty) other;
    return Objects.equals(delta, o.delta);
  }

  public static class TvrOuterJoinViewDonePropertySerde
      implements JsonDeserializer<TvrOuterJoinViewDoneProperty>,
      JsonSerializer<TvrOuterJoinViewDoneProperty> {

    public static TvrOuterJoinViewDonePropertySerde instance =
        new TvrOuterJoinViewDonePropertySerde();

    @Override public TvrOuterJoinViewDoneProperty deserialize(JsonElement json,
        Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      TvrVersion fromVersion = context.deserialize(jsonObject.get("fromVersion"), TvrVersion.class);
      TvrVersion toVersion = context.deserialize(jsonObject.get("toVersion"), TvrVersion.class);
      return new TvrOuterJoinViewDoneProperty(VersionInterval.of(fromVersion, toVersion));
    }

    @Override public JsonElement serialize(TvrOuterJoinViewDoneProperty src,
        Type typeOfSrc, JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();
      jsonObject.add("fromVersion", context.serialize(src.delta.from, TvrVersion.class));
      jsonObject.add("toVersion", context.serialize(src.delta.to, TvrVersion.class));
      return jsonObject;
    }
  }

}
