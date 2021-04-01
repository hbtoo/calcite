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
package org.apache.calcite.rel.tvr.trait;

import org.apache.calcite.plan.tvr.TvrVersion;
import org.apache.calcite.plan.volcano.TvrMetaSetType;
import org.apache.calcite.rel.tvr.utils.TvrContext;
import org.apache.calcite.rel.tvr.utils.TvrUtils;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.VersionInterval;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.LongStream;

/**
 * The snapshot series with three tables and three versions is:
 * 111
 * 222
 * 333
 */
public class TvrDefaultMetaSetType extends TvrMetaSetType {

  private long[] versions;
  private int versionDim;

  private boolean bigDeltaEnabled;

  private TvrDefaultMetaSetType(long[] versions, int versionDim,
      boolean bigDeltaEnabled) {
    super(createVersions(versions, versionDim, bigDeltaEnabled));
    this.versions = versions;
    this.versionDim = versionDim;
    this.bigDeltaEnabled = bigDeltaEnabled;
  }

  public static TvrDefaultMetaSetType create(long[] versions, int versionDim,
      TvrContext ctx) {
    return new TvrDefaultMetaSetType(versions, versionDim,
        TvrUtils.progressiveBigDeltaEnabled(ctx));
  }

  private static Pair<TvrVersion[], VersionInterval[]> createVersions(
      long[] versions, int versionDim, boolean bigDeltaEnabled) {
    int instantNum = versions.length;
    TvrVersion[] snapshots = new TvrVersion[instantNum];
    for (int i = 0; i < instantNum; ++i) {
      long[] snapshot = new long[versionDim];
      long instant = versions[i];
      for (int j = 0; j < versionDim; ++j) {
        snapshot[j] = instant;
      }
      snapshots[i] = TvrVersion.of(snapshot);
    }

    VersionInterval[] deltas;
    if (bigDeltaEnabled) {
      deltas = new VersionInterval[(instantNum - 1) * instantNum / 2];
      int k = 0;
      for (int i = 0; i < instantNum; i++) {
        for (int j = i + 1; j < instantNum; j++) {
          deltas[k++] = VersionInterval.of(snapshots[i], snapshots[j]);
        }
      }
    } else {
      deltas = new VersionInterval[instantNum - 1];
      for (int i = 0; i < deltas.length; ++i) {
        deltas[i] = VersionInterval.of(snapshots[i], snapshots[i + 1]);
      }
    }
    return Pair.of(snapshots, deltas);
  }

  @Override public long[] getTvrVersions() {
    return versions;
  }

  public int getVersionDim() {
    return versionDim;
  }

  @Override public TvrDefaultMetaSetType copy() {
    return new TvrDefaultMetaSetType(versions, versionDim, bigDeltaEnabled);
  }

  @Override public boolean addNewDimensions(int newVersionDim,
      Map<Integer, Integer> ordinalMapping) {
    // ignore newVersionDim, return the same type
    if (newVersionDim == versionDim) {
      return false;
    }
    this.versionDim = newVersionDim;
    Pair<TvrVersion[], VersionInterval[]> versions =
        createVersions(getTvrVersions(), newVersionDim, bigDeltaEnabled);
    this.snapshots = versions.left;
    this.deltas = versions.right;
    return true;
  }

  @Override public boolean addNewVersion(long newVersion) {
    int insertIndex = Arrays.binarySearch(versions, newVersion);
    if (insertIndex >= 0) {
      return false;
    }

    long[] newVersions = LongStream
        .concat(Arrays.stream(getTvrVersions()), LongStream.of(newVersion))
        .sorted().toArray();

    Pair<TvrVersion[], VersionInterval[]> pair = createVersions(
        newVersions, versionDim, bigDeltaEnabled);
    this.versions = newVersions;
    this.snapshots = pair.left;
    this.deltas = pair.right;
    return true;
  }

}
