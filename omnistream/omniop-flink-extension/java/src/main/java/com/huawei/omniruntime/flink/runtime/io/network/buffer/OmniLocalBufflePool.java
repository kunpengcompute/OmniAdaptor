/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.huawei.omniruntime.flink.runtime.io.network.buffer;

import java.util.concurrent.CompletableFuture;

import javax.annotation.concurrent.GuardedBy;

/**
 * OmniLocalBufflePool
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class OmniLocalBufflePool extends OmniBufferPoolShadow {
    @GuardedBy("availableMemorySegments")
    private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

    public OmniLocalBufflePool(long nativeBufferPoolAddress) {
        super(nativeBufferPoolAddress);
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }
}
