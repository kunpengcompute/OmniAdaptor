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
import com.huawei.omniruntime.flink.runtime.metrics.OmniTimeGauge;
import com.huawei.omniruntime.flink.runtime.metrics.utils.OmniMetricHelper;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * Java-side metric group for a native LocalNettyBufferPool.
 *
 * <p>The gauges in this group are native SizeGauge instances backed by a LocalNettyBufferPool. The
 * expected Flink scope is:
 *
 * <pre>
 * task.Shuffle.Netty.LocalNettyBufferPool.&lt;poolName&gt;.&lt;metricName&gt;
 * </pre>
 */
public class OmniTaskLocalNettyBufferMetricGroup extends AbstractMetricGroup<TaskMetricGroup> implements MetricCloseable {
    private static final String METRIC_GROUP_NAME = "OmniLocalNettyBufferMetricGroup";
    private static final String SHUFFLE_GROUP_NAME = "Shuffle";
    private static final String NETTY_GROUP_NAME = "Netty";
    private static final String LOCAL_NETTY_BUFFER_POOL_GROUP_NAME = "LocalNettyBufferPool";

    public static final String REQUIRED_BUFFERS = "requiredBuffers";
    public static final String MAX_BUFFERS = "maxBuffers";
    public static final String CURRENT_POOL_SIZE = "currentPoolSize";
    public static final String AVAILABLE_BUFFERS = "availableBuffers";
    public static final String REQUESTED_BUFFERS = "requestedBuffers";
    public static final String REQUEST_REGULAR_BUFFER_COUNT = "requestRegularBufferCount";
    public static final String REQUEST_BIG_BUFFER_COUNT = "requestBigBufferCount";
    public static final String RECYCLE_REGULAR_BUFFER_COUNT = "recycleRegularBufferCount";
    public static final String RECYCLE_BIG_BUFFER_COUNT = "recycleBigBufferCount";
    public static final String BIG_TOTAL_MEMORY_SIZE = "bigTotalMemorySize";
    public static final String AVAILABLE_BIG_MEMORY_SIZE = "availableBigMemorySize";

    private final long nativeCurrentMetricGroupRef;
    private final List<MetricCloseable> closeables = new ArrayList<>();


    public OmniTaskLocalNettyBufferMetricGroup(TaskMetricGroup taskMetricGroup, long nativeTaskMetricGroupRef,long nativeTaskRef) {
        super(OmniMetricHelper.getMetricRegistry(taskMetricGroup),
                createScopeComponents(taskMetricGroup),
                taskMetricGroup);
        nativeCurrentMetricGroupRef = addLocalNettyBufferPoolMetricGroup(nativeTaskMetricGroupRef,nativeTaskRef,METRIC_GROUP_NAME,createScopeComponents(taskMetricGroup));
        createOmniMetricsInstance();
    }

    private static String[] createScopeComponents(TaskMetricGroup parent) {
        String[] parentScope = parent.getScopeComponents();
        String[] scope = new String[parentScope.length + 4];
        System.arraycopy(parentScope, 0, scope, 0, parentScope.length);
        scope[parentScope.length] = SHUFFLE_GROUP_NAME;
        scope[parentScope.length + 1] = NETTY_GROUP_NAME;
        scope[parentScope.length + 2] = LOCAL_NETTY_BUFFER_POOL_GROUP_NAME;
        return scope;
    }

    private static String normalizePoolName(String poolName) {
        if (poolName == null || poolName.trim().isEmpty()) {
            return "unknown";
        }
        return poolName;
    }

    private void createOmniMetricsInstance() {
        registerSizeGauge(REQUIRED_BUFFERS);
        registerSizeGauge(MAX_BUFFERS);
        registerSizeGauge(CURRENT_POOL_SIZE);
        registerSizeGauge(AVAILABLE_BUFFERS);
        registerSizeGauge(REQUESTED_BUFFERS);
        registerSizeGauge(REQUEST_REGULAR_BUFFER_COUNT);
        registerSizeGauge(REQUEST_BIG_BUFFER_COUNT);
        registerSizeGauge(RECYCLE_REGULAR_BUFFER_COUNT);
        registerSizeGauge(RECYCLE_BIG_BUFFER_COUNT);
        registerSizeGauge(BIG_TOTAL_MEMORY_SIZE);
        registerSizeGauge(AVAILABLE_BIG_MEMORY_SIZE);
    }

    private void registerSizeGauge(String metricName) {
        OmniSizeGauge gauge = OmniMetricHelper.createSizeGauge(nativeCurrentMetricGroupRef, getMetricGroupName(), metricName);
        gauge(metricName, gauge);
        closeables.add(gauge);
    }

    private String getMetricGroupName() {
        return METRIC_GROUP_NAME;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return String.join(String.valueOf(registry.getDelimiter()),
                filter.filterCharacters(SHUFFLE_GROUP_NAME),
                filter.filterCharacters(NETTY_GROUP_NAME),
                filter.filterCharacters(LOCAL_NETTY_BUFFER_POOL_GROUP_NAME));
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

    public static native  long addLocalNettyBufferPoolMetricGroup(long parentGroupRef,long nativeTaskRef,String groupName,String[] scope);

}
