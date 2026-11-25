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

import org.apache.flink.metrics.Counter;

import java.util.ArrayList;
import java.util.List;

/**
 * OmniSumCounter is a specialized counter that aggregates the counts of multiple internal counters.
 * It extends the OmniSimpleCounter class and implements the MetricCloseable interface.
 * This class is used to sum up the counts from different sources and provide a single count value.
 *
 * @since 2025-04-16
 */
public class OmniSumCounter extends OmniSimpleCounter implements MetricCloseable {
    private final List<Counter> internalCounters = new ArrayList<>();

    public OmniSumCounter(long nativeRef) {
        super(nativeRef);
    }

    /**
     * Adds a Counter to the list of internal counters to be summed.
     *
     * @param toAdd The Counter to add.
     */
    public void addCounter(Counter toAdd) {
        internalCounters.add(toAdd);
    }

    /**
     * get count value
     *
     * @return count
     */
    @Override
    public long getCount() {
        long sum = 0L;
        for (Counter counter : internalCounters) {
            sum += counter.getCount();
        }
        return sum;
    }
}
