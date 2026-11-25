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

package com.huawei.omniruntime.flink.runtime.metrics;

import org.apache.flink.metrics.Gauge;

/**
 * OmniLongSizeGauge is the 64-bit counterpart of {@link OmniSizeGauge}. It is used for byte-valued
 * metrics (keyed-state data sizes) whose value can exceed {@link Integer#MAX_VALUE}; the int-typed
 * OmniSizeGauge is kept unchanged for counts and buffer-pool sizes.
 *
 * @since 2025-06-10
 */

public class OmniLongSizeGauge implements Gauge<Long>, MetricCloseable {
    private long nativeRef = 0L;
    private volatile boolean isClosed = false;
    private volatile long originalValue = 0L;

    public OmniLongSizeGauge(long nativeRef) {
        this.nativeRef = nativeRef;
    }

    /**
     * get size.
     *
     * @return value
     */
    @Override
    public Long getValue() {
        if (isClosed) {
            return originalValue;
        } else {
            originalValue = getNativeSize(nativeRef);
        }
        return originalValue;
    }

    /**
     * close the gauge.
     */
    @Override
    public void close() {
        // jni call to close the counter
        isClosed = true;
    }

    /**
     * get native reference.
     *
     * @param nativeRef native reference
     * @return native size
     */
    public native long getNativeSize(long nativeRef);
}
