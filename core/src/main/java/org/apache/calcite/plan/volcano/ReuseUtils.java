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

import static org.apache.calcite.plan.volcano.MatType.MAT_DISK;
import static org.apache.calcite.plan.volcano.MatType.MAT_MEMORY;

class ReuseUtils {

  private ReuseUtils() {
  }

  enum UseType {
    TO_MEMORY,  // require table sink
    TO_DISK,    // require table sink
    TO_OP       // require the cheapest sub-plan (with the same time)
  }

  static UseType matType2UseType(MatType matType) {
    switch (matType) {
    case MAT_DISK:
      return UseType.TO_DISK;
    case MAT_MEMORY:
      return UseType.TO_MEMORY;
    default:
      throw new IllegalArgumentException("Illegal MatType " + matType);
    }
  }

  static MatType useType2matType(UseType useType) {
    switch (useType) {
    case TO_DISK:
      return MAT_DISK;
    case TO_MEMORY:
      return MAT_MEMORY;
    default:
      throw new IllegalArgumentException("Illegal UseType " + useType);
    }
  }

}
