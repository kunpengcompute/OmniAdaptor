/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.metrics.groups;

import com.huawei.omniruntime.flink.runtime.metrics.MetricCloseable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a metric group for managing and registering metrics in the
 * OmniRuntime environment.
 * This class provides functionality to register metrics and retrieve
 * information about the metric group.
 *
 * @since 2025-04-16
 */
public class OmniTaskMetricGroup {
    private Map<String, OmniInternalOperatorIOMetricGroup> operators = new HashMap<>();

    private OmniTaskIOMetricGroup ioMetrics;

    // Metric groups whose gauges read through a raw native OmniTask pointer. They must be closed
    // before that task is deleted, so they are collected here to be reachable from close().
    private final List<MetricCloseable> nativeTaskBackedGroups = new ArrayList<>();

    /**
     * set the task metric group.
     *
     * @param ioMetrics the task metric group
     */
    public void setOmniTaskIOMetricGroup(OmniTaskIOMetricGroup ioMetrics) {
        this.ioMetrics = ioMetrics;
    }

    /**
     * get the task metric group.
     *
     * @return the task metric group
     */
    public OmniTaskIOMetricGroup getOmniTaskIOMetricGroup() {
        return ioMetrics;
    }

    /**
     * get the operator metric group.
     *
     * @param operatorName the operator name
     * @param operator the operator metric group
     */
    public void addOperator(String operatorName, OmniInternalOperatorIOMetricGroup operator) {
        operators.put(operatorName, operator);
    }

    /**
     * add a metric group backed by the native task, so that it is closed together with this group.
     *
     * @param group the metric group to close
     */
    public void addNativeTaskBackedGroup(MetricCloseable group) {
        nativeTaskBackedGroups.add(group);
    }

    /**
     * close the metric group.
     */
    public void close() {
        for (OmniInternalOperatorIOMetricGroup operator : operators.values()) {
            operator.close();
        }
        if (ioMetrics != null) {
            ioMetrics.close();
        }
        for (MetricCloseable group : nativeTaskBackedGroups) {
            group.close();
        }
        nativeTaskBackedGroups.clear();
    }
}
