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

import org.apache.calcite.plan.volcano.TvrProperty;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Objects;

public class TvrUpdateOneTableProperty extends TvrProperty {

  public enum PropertyType {
    DB_TOASTER, OJV,
  }

  private int changingTable;

  // Which algorithm this property is used for
  private PropertyType type;

  public TvrUpdateOneTableProperty(int changingTable, PropertyType type) {
    this.changingTable = changingTable;
    this.type = type;
  }

  public int getChangingTable() {
    return changingTable;
  }

  public PropertyType getType() {
    return type;
  }

  @Override public String toString() {
    return changingTable + ":" + type;
  }

  @Override public int hashCode() {
    return Objects.hash(changingTable, type);
  }

  @Override public boolean equals(Object other) {
    if (!(other instanceof TvrUpdateOneTableProperty)) {
      return false;
    }
    TvrUpdateOneTableProperty o = (TvrUpdateOneTableProperty) other;
    return Objects.equals(changingTable, o.changingTable) && Objects
        .equals(type, o.type);
  }

  public static class TvrUpdateOneTablePropertySerde
      implements JsonDeserializer<TvrUpdateOneTableProperty>,
      JsonSerializer<TvrUpdateOneTableProperty> {

    public static TvrUpdateOneTablePropertySerde instance =
        new TvrUpdateOneTablePropertySerde();

    @Override public TvrUpdateOneTableProperty deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      int changingTable = jsonObject.get("changingTable").getAsInt();
      String propertyType = jsonObject.get("propertyType").getAsString();
      return new TvrUpdateOneTableProperty(changingTable,
          PropertyType.valueOf(propertyType));
    }

    @Override public JsonElement serialize(TvrUpdateOneTableProperty src, Type typeOfSrc,
        JsonSerializationContext context) {
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("changingTable", src.changingTable);
      jsonObject.addProperty("propertyType", src.type.name());
      return jsonObject;
    }
  }

}
