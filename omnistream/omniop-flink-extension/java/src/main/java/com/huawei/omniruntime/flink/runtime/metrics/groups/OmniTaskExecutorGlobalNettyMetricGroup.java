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
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;

/** Java-side metric group for native GlobalNettyBufferPool metrics owned by the task executor. */
public class OmniTaskExecutorGlobalNettyMetricGroup
        extends AbstractMetricGroup<TaskManagerMetricGroup> implements MetricCloseable {
    private static final String METRIC_GROUP_NAME = "OmniTaskExecutorGlobalNettyMetricGroup";
    private static final String SHUFFLE_GROUP_NAME = "Shuffle";
    private static final String NETTY_GROUP_NAME = "Netty";
    private static final String GLOBAL_NETTY_BUFFER_POOL_GROUP_NAME = "GlobalNettyBufferPool";

    public static final String TOTAL_NUMBER_OF_BUFFERS = "totalNumberOfBuffers";
    public static final String ALLOCATED_REGULAR_BUFFER_COUNT = "allocatedRegularBufferCount";
    public static final String NUM_TOTAL_REQUIRED_BUFFERS = "numTotalRequiredBuffers";
    public static final String ALL_LOCAL_POOLS_SIZE = "allLocalPoolsSize";
    public static final String AVAILABLE_BUFFERS = "availableBuffers";

    private final long nativeCurrentMetricGroupRef;
    private final List<MetricCloseable> closeables = new ArrayList<>();

    public OmniTaskExecutorGlobalNettyMetricGroup(
            TaskManagerMetricGroup taskManagerMetricGroup,
            long nativeTaskManagerMetricGroupRef,
            long nativeTaskExecutorRef) {
        super(
                OmniMetricHelper.getMetricRegistry(taskManagerMetricGroup),
                createScopeComponents(taskManagerMetricGroup),
                taskManagerMetricGroup);
        nativeCurrentMetricGroupRef =
                addTaskExecutorGlobalNettyMetricGroup(
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
        scope[parentScope.length + 1] = NETTY_GROUP_NAME;
        scope[parentScope.length + 2] = GLOBAL_NETTY_BUFFER_POOL_GROUP_NAME;
        return scope;
    }

    private void createOmniMetricsInstance() {
        registerSizeGauge(TOTAL_NUMBER_OF_BUFFERS);
        registerSizeGauge(ALLOCATED_REGULAR_BUFFER_COUNT);
        registerSizeGauge(NUM_TOTAL_REQUIRED_BUFFERS);
        registerSizeGauge(ALL_LOCAL_POOLS_SIZE);
        registerSizeGauge(AVAILABLE_BUFFERS);
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
                filter.filterCharacters(NETTY_GROUP_NAME),
                filter.filterCharacters(GLOBAL_NETTY_BUFFER_POOL_GROUP_NAME));
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

    public static native long addTaskExecutorGlobalNettyMetricGroup(
            long parentGroupRef, long nativeTaskExecutorRef, String groupName, String[] scope);
}
