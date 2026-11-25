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

package com.huawei.omniruntime.flink.runtime.metrics.groups;

import com.huawei.omniruntime.flink.runtime.metrics.MetricCloseable;
import com.huawei.omniruntime.flink.runtime.metrics.OmniSizeGauge;
import com.huawei.omniruntime.flink.runtime.metrics.utils.OmniMetricHelper;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * Java-side metric group for native LocalObjectBufferPool metrics used by VectorBatch buffers.
 */
public class VectorBatchBufferPoolMetricGroup extends AbstractMetricGroup<TaskMetricGroup>
        implements MetricCloseable {
    private static final String METRIC_GROUP_NAME = "VectorBatchBufferPoolMetricGroup";
    private static final String SHUFFLE_GROUP_NAME = "Shuffle";
    private static final String VECTOR_BATCH_GROUP_NAME = "VectorBatch";
    private static final String LOCAL_OBJECT_BUFFER_POOL_GROUP_NAME = "LocalObjectBufferPool";

    public static final String OBJECT_SEGMENT_SIZE = "objectSegmentSize";
    public static final String REQUIRED_MEMORY = "requiredMemory";
    public static final String CURRENT_POOL_MEMORY_BUDGET = "currentPoolMemoryBudget";
    public static final String MAX_ALLOWED_MEMORY = "maxAllowedMemory";
    public static final String USED_MEMORY = "usedMemory";
    public static final String AVAILABLE_MEMORY = "availableMemory";
    public static final String MAX_MEMORY_PER_CHANNEL = "maxMemoryPerChannel";
    public static final String REQUEST_SEGMENT_NUMBER = "requestSegmentNumber";
    public static final String RECYCLE_SEGMENT_NUMBER = "recycleSegmentNumber";

    private final long nativeCurrentMetricGroupRef;
    private final List<MetricCloseable> closeables = new ArrayList<>();

    public VectorBatchBufferPoolMetricGroup(
            TaskMetricGroup taskMetricGroup, long nativeTaskMetricGroupRef, long nativeTaskRef) {
        super(
                OmniMetricHelper.getMetricRegistry(taskMetricGroup),
                createScopeComponents(taskMetricGroup),
                taskMetricGroup);
        nativeCurrentMetricGroupRef =
                addVectorBatchBufferPoolMetricGroup(
                        nativeTaskMetricGroupRef,
                        nativeTaskRef,
                        METRIC_GROUP_NAME,
                        createScopeComponents(taskMetricGroup));
        createOmniMetricsInstance();
    }

    private static String[] createScopeComponents(TaskMetricGroup parent) {
        String[] parentScope = parent.getScopeComponents();
        String[] scope = new String[parentScope.length + 3];
        System.arraycopy(parentScope, 0, scope, 0, parentScope.length);
        scope[parentScope.length] = SHUFFLE_GROUP_NAME;
        scope[parentScope.length + 1] = VECTOR_BATCH_GROUP_NAME;
        scope[parentScope.length + 2] = LOCAL_OBJECT_BUFFER_POOL_GROUP_NAME;
        return scope;
    }

    private void createOmniMetricsInstance() {
        registerSizeGauge(OBJECT_SEGMENT_SIZE);
        registerSizeGauge(REQUIRED_MEMORY);
        registerSizeGauge(CURRENT_POOL_MEMORY_BUDGET);
        registerSizeGauge(MAX_ALLOWED_MEMORY);
        registerSizeGauge(USED_MEMORY);
        registerSizeGauge(AVAILABLE_MEMORY);
        registerSizeGauge(MAX_MEMORY_PER_CHANNEL);
        registerSizeGauge(REQUEST_SEGMENT_NUMBER);
        registerSizeGauge(RECYCLE_SEGMENT_NUMBER);
    }

    private void registerSizeGauge(String metricName) {
        OmniSizeGauge gauge =
                OmniMetricHelper.createSizeGauge(
                        nativeCurrentMetricGroupRef, getMetricGroupName(), metricName);
        gauge(metricName, gauge);
        closeables.add(gauge);
    }

    private String getMetricGroupName() {
        return METRIC_GROUP_NAME;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return String.join(
                String.valueOf(registry.getDelimiter()),
                filter.filterCharacters(SHUFFLE_GROUP_NAME),
                filter.filterCharacters(VECTOR_BATCH_GROUP_NAME),
                filter.filterCharacters(LOCAL_OBJECT_BUFFER_POOL_GROUP_NAME));
    }

    @Override
    protected QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
        return parent.getQueryServiceMetricInfo(filter).copy(getGroupName(filter));
    }

    @Override
    public void close() {
        for (MetricCloseable closeable : closeables) {
            closeable.close();
        }
        closeables.clear();
        super.close();
    }

    public static native long addVectorBatchBufferPoolMetricGroup(
            long parentGroupRef, long nativeTaskRef, String groupName, String[] scope);
}
