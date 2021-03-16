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

/**
 * Time matching policy for a group of TvrEdgeRelOptRuleOperands. All edgeOps
 * sharing the same instance of TvrEdgeTimeMatchInfo is considered a group.
 */
public class TvrEdgeTimeMatchInfo {

  /**
   * Time matching policy for multiple TvrEdgeRelOptRuleOperands.
   */
  public enum TvrEdgeTimeMatchPolicy {

    /**
     * All tvr edges are of the exact same time interval
     */
    IDENTICAL,

    /**
     * All tvr edge time intervals are adjacent to each other, ascending order:
     * tvrEdge1.toVersion == tvrEdge2.fromVersion
     * tvrEdge2.toVersion == tvrEdge3.fromVersion
     * ...
     */
    ADJACENT,

  }

  public TvrEdgeTimeMatchInfo(TvrEdgeTimeMatchPolicy policy) {
    this.policy = policy;
  }

  public final TvrEdgeTimeMatchPolicy policy;

}
