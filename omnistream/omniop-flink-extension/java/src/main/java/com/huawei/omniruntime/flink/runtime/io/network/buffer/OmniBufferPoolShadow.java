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

package com.huawei.omniruntime.flink.runtime.io.network.buffer;

import java.util.concurrent.CompletableFuture;

/**
 * OmniBufferPoolShadow
 *
 * @since 2025-04-27
 */
public class OmniBufferPoolShadow implements OmniBufferPool {
    /**
     * refers to an address pointing to a memory buffer pool in the underlying system
     */
    protected long nativeBufferPoolAddress;

    public OmniBufferPoolShadow(long nativeBufferPoolAddress) {
        this.nativeBufferPoolAddress = nativeBufferPoolAddress;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAvailable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isApproximatelyAvailable() {
        throw new UnsupportedOperationException();
    }
}
