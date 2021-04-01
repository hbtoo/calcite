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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Related information used for outer join view rules. (Efficient Maintenance of
 * Materialized Outer-Join Views, ICDE'07)
 *
 * This tvr property is used as a self loop on tvr. It is considered as a commit
 * message, i.e. all {@link TvrUpdateOneTableProperty} linked for this tvr has
 * been fully connected.
 */
public class TvrOuterJoinViewProperty extends TvrProperty {

  // List of possible terms (combination of source tables) in the result
  // The list is sorted so that this.equals() works properly
  private List<LinkedHashSet<Integer>> terms;

  // Trace the provenance of each column, i.e. which source table the column
  // comes from. Empty set means this column is a constant.
  private List<LinkedHashSet<Integer>> nonNullTermTables;

  // Ordered (pre-order in the rel tree) list of changing tables
  private LinkedHashSet<Integer> changingTables;

  public TvrOuterJoinViewProperty(List<LinkedHashSet<Integer>> terms,
      List<LinkedHashSet<Integer>> nonNullTermTables,
      Set<Integer> changingTables) {
    // Sort the terms list so that this.equals() works properly
    this.terms = new ArrayList<>(terms);
    this.terms.sort((set1, set2) -> {
      if (set1.size() != set2.size()) {
        return set1.size() - set2.size();
      }
      // Assume both sets are sorted already, so compare each item one by one
      Iterator<Integer> iter1 = set1.iterator();
      Iterator<Integer> iter2 = set2.iterator();
      while (iter1.hasNext()) {
        int n = Integer.compare(iter1.next(), iter2.next());
        if (n != 0) {
          return n;
        }
      }
      return 0;
    });

    this.nonNullTermTables = nonNullTermTables;
    this.changingTables = new LinkedHashSet<>(changingTables);
  }

  public List<LinkedHashSet<Integer>> getTerms() {
    return this.terms;
  }

  public List<LinkedHashSet<Integer>> getNonNullTermTables() {
    return this.nonNullTermTables;
  }

  public LinkedHashSet<Integer> allChangingTermTables() {
    return changingTables;
  }

  @Override public String toString() {
    return "OJV:" + changingTables.toString();
  }

  public String toFullString() {
    return terms.toString() + " -- " + changingTables.toString() + " -- "
        + nonNullTermTables.toString();
  }

  @Override public int hashCode() {
    return Objects.hash(terms, nonNullTermTables, changingTables);
  }

  @Override public boolean equals(Object other) {
    if (!(other instanceof TvrOuterJoinViewProperty)) {
      return false;
    }
    TvrOuterJoinViewProperty o = (TvrOuterJoinViewProperty) other;
    return Objects.equals(terms, o.terms) && Objects
        .equals(nonNullTermTables, o.nonNullTermTables) && Objects
        .equals(changingTables, o.changingTables);
  }

  public static LinkedHashSet<Integer> getOrderedTableSet(
      Collection<Integer> term) {
    List<Integer> list = new ArrayList<>(term);
    list.sort(Comparator.naturalOrder());
    return new LinkedHashSet<>(list);
  }

  public static class TvrOuterJoinViewPropertySerde
      implements JsonDeserializer<TvrOuterJoinViewProperty>,
      JsonSerializer<TvrOuterJoinViewProperty> {

    public static TvrOuterJoinViewPropertySerde instance = new TvrOuterJoinViewPropertySerde();

    @Override public TvrOuterJoinViewProperty deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      return new TvrOuterJoinViewProperty(ImmutableList.of(),
          ImmutableList.of(), ImmutableSet.of());
    }

    @Override public JsonElement serialize(TvrOuterJoinViewProperty src, Type typeOfSrc,
        JsonSerializationContext context) {
      return new JsonObject();
    }
  }

}
