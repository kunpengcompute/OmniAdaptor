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

import java.util.function.LongConsumer;

/** Owns the registry reference for one slot pool, shared by MemoryManager leases. */
public final class OmniRocksDBSharedResources implements AutoCloseable {
    private long handle;
    private final long totalMemoryBytes;
    final OmniRocksDBMemoryConfiguration configuration;
    private final LongConsumer releaser;

    OmniRocksDBSharedResources(long handle, long totalMemoryBytes,
            OmniRocksDBMemoryConfiguration configuration, LongConsumer releaser) {
        if (handle <= 0 || totalMemoryBytes <= 0) {
            throw new IllegalArgumentException("Invalid Omni RocksDB shared resource");
        }
        this.handle = handle;
        this.totalMemoryBytes = totalMemoryBytes;
        this.configuration = configuration;
        this.releaser = releaser;
    }

    public synchronized long getHandle() {
        if (handle == 0) {
            throw new IllegalStateException("Omni RocksDB shared resource is closed");
        }
        return handle;
    }

    public long getTotalMemoryBytes() {
        return totalMemoryBytes;
    }

    @Override
    public synchronized void close() {
        if (handle != 0) {
            releaser.accept(handle);
            handle = 0;
        }
    }
}
