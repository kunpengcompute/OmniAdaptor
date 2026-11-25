/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * OmniAdaptor is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

package com.huawei.omniruntime.flink.runtime.state;

import org.apache.flink.contrib.streaming.state.RocksDBMemoryConfiguration;

/** Immutable memory policy; contains no FRocksDB native objects. */
public final class OmniRocksDBMemoryConfiguration {
    final boolean managed;
    final long fixedBytes;
    final double writeBufferRatio;
    final double highPriorityRatio;
    final boolean partitionedFilters;

    public OmniRocksDBMemoryConfiguration(RocksDBMemoryConfiguration configuration) {
        this(configuration.isUsingManagedMemory(),
                configuration.isUsingFixedMemoryPerSlot()
                        ? configuration.getFixedMemoryPerSlot().getBytes() : 0,
                configuration.getWriteBufferRatio(), configuration.getHighPriorityPoolRatio(),
                configuration.isUsingPartitionedIndexFilters());
    }

    OmniRocksDBMemoryConfiguration(boolean managed, long fixedBytes, double writeBufferRatio,
            double highPriorityRatio, boolean partitionedFilters) {
        if (fixedBytes < 0 || !Double.isFinite(writeBufferRatio) || writeBufferRatio <= 0
                || writeBufferRatio >= 1 || !Double.isFinite(highPriorityRatio)
                || highPriorityRatio < 0 || highPriorityRatio >= 1
                || 2 * writeBufferRatio / (3 - writeBufferRatio) + highPriorityRatio >= 1) {
            throw new IllegalArgumentException("Invalid Omni RocksDB memory configuration");
        }
        this.managed = managed;
        this.fixedBytes = fixedBytes;
        this.writeBufferRatio = writeBufferRatio;
        this.highPriorityRatio = highPriorityRatio;
        this.partitionedFilters = partitionedFilters;
    }

    boolean matches(OmniRocksDBMemoryConfiguration other) {
        return managed == other.managed && fixedBytes == other.fixedBytes
                && Double.compare(writeBufferRatio, other.writeBufferRatio) == 0
                && Double.compare(highPriorityRatio, other.highPriorityRatio) == 0
                && partitionedFilters == other.partitionedFilters;
    }
}
