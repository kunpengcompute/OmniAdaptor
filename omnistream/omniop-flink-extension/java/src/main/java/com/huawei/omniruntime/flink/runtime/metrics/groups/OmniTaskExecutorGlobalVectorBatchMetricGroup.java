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
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;

import java.util.ArrayList;
import java.util.List;

/** Java-side metric group for native NetworkObjectBufferPool metrics owned by the task executor. */
public class OmniTaskExecutorGlobalVectorBatchMetricGroup
        extends AbstractMetricGroup<TaskManagerMetricGroup> implements MetricCloseable {
    private static final String METRIC_GROUP_NAME = "OmniTaskExecutorGlobalVectorBatchMetricGroup";
    private static final String SHUFFLE_GROUP_NAME = "Shuffle";
    private static final String VECTOR_BATCH_GROUP_NAME = "VectorBatch";
    private static final String GLOBAL_OBJECT_BUFFER_POOL_GROUP_NAME = "NetworkObjectBufferPool";

    public static final String OBJECT_SEGMENT_SIZE = "objectSegmentSize";
    public static final String TOTAL_NUMBER_OF_OBJECT_SEGMENTS = "totalNumberOfObjectSegments";
    public static final String TOTAL_MEMORY = "totalMemory";
    public static final String AVAILABLE_OBJECT_SEGMENTS = "availableObjectSegments";
    public static final String AVAILABLE_MEMORY = "availableMemory";
    public static final String USED_OBJECT_SEGMENTS = "usedObjectSegments";
    public static final String USED_MEMORY = "usedMemory";
    public static final String REGISTERED_BUFFER_POOLS = "registeredBufferPools";
    public static final String BUFFER_COUNT = "bufferCount";

    private final long nativeCurrentMetricGroupRef;
    private final List<MetricCloseable> closeables = new ArrayList<>();

    public OmniTaskExecutorGlobalVectorBatchMetricGroup(
            TaskManagerMetricGroup taskManagerMetricGroup,
            long nativeTaskManagerMetricGroupRef,
            long nativeTaskExecutorRef) {
        super(
                OmniMetricHelper.getMetricRegistry(taskManagerMetricGroup),
                createScopeComponents(taskManagerMetricGroup),
                taskManagerMetricGroup);
        nativeCurrentMetricGroupRef =
                addTaskExecutorGlobalVectorBatchMetricGroup(
                        nativeTaskManagerMetricGroupRef,
                        nativeTaskExecutorRef,
                        METRIC_GROUP_NAME,
                        createScopeComponents(taskManagerMetricGroup));
        createOmniMetricsInstance();
    }

    private static String[] createScopeComponents(TaskManagerMetricGroup parent) {
        String[] parentScope = parent.getScopeComponents();
        String[] scope = new String[parentScope.length + 3];
        System.arraycopy(parentScope, 0, scope, 0, parentScope.length);
        scope[parentScope.length] = SHUFFLE_GROUP_NAME;
        scope[parentScope.length + 1] = VECTOR_BATCH_GROUP_NAME;
        scope[parentScope.length + 2] = GLOBAL_OBJECT_BUFFER_POOL_GROUP_NAME;
        return scope;
    }

    private void createOmniMetricsInstance() {
        registerSizeGauge(OBJECT_SEGMENT_SIZE);
        registerSizeGauge(TOTAL_NUMBER_OF_OBJECT_SEGMENTS);
        registerSizeGauge(TOTAL_MEMORY);
        registerSizeGauge(AVAILABLE_OBJECT_SEGMENTS);
        registerSizeGauge(AVAILABLE_MEMORY);
        registerSizeGauge(USED_OBJECT_SEGMENTS);
        registerSizeGauge(USED_MEMORY);
        registerSizeGauge(REGISTERED_BUFFER_POOLS);
        registerSizeGauge(BUFFER_COUNT);
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
                filter.filterCharacters(GLOBAL_OBJECT_BUFFER_POOL_GROUP_NAME));
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

    public static native long addTaskExecutorGlobalVectorBatchMetricGroup(
            long parentGroupRef, long nativeTaskExecutorRef, String groupName, String[] scope);
}
