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

/**
 * Interface for classes that can be closed to release resources.
 * This is typically used for metrics that need to clean up native resources.
 *
 * @since 2025-04-16
 */
public interface MetricCloseable {
    /**
     * Closes the metric and releases any associated resources.
     * This method should be called when the metric is no longer needed.
     */
    default void close() {
    }
}
