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
import com.huawei.omniruntime.flink.runtime.metrics.OmniLongSizeGauge;
import com.huawei.omniruntime.flink.runtime.metrics.OmniSizeGauge;
import com.huawei.omniruntime.flink.runtime.metrics.utils.OmniMetricHelper;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Java-side metric group for the native per-operator keyed-state metrics. The gauges are native
 * SizeGauge instances backed by a native OperatorStateMetricGroup.
 *
 * <p>The scope is generated the same way Flink's {@code TaskMetricGroup.getOrAddOperator} scopes an
 * operator: the operator name is truncated to {@link #METRICS_OPERATOR_NAME_MAX_LENGTH} characters
 * and the OperatorID + operator name are exposed as scope variables. The resulting Flink scope is:
 *
 * <pre>
 * task.&lt;operatorName&gt;.BackendState.&lt;metricName&gt;
 * </pre>
 */
public class OmniOperatorStateMetricGroup extends AbstractMetricGroup<TaskMetricGroup> implements MetricCloseable {
    /** Mirrors Flink TaskMetricGroup.METRICS_OPERATOR_NAME_MAX_LENGTH. */
    private static final int METRICS_OPERATOR_NAME_MAX_LENGTH = 80;
    private static final String METRIC_GROUP_NAME = "OmniOperatorStateMetricGroup";
    private static final String BACKEND_STATE_GROUP_NAME = "BackendState";

    public static final String VALUE_STATE_COUNT = "valueStateCount";
    public static final String MAP_STATE_COUNT = "mapStateCount";
    public static final String LIST_STATE_COUNT = "listStateCount";
    public static final String VALUE_STATE_DATA_SIZE = "valueStateDataSize";
    public static final String MAP_STATE_DATA_SIZE = "mapStateDataSize";
    public static final String LIST_STATE_DATA_SIZE = "listStateDataSize";
    // VectorBatch buffers held in keyed state (heap backend only; RocksDB reports 0) + grand total.
    public static final String VECTOR_BATCH_STATE_DATA_SIZE = "vectorBatchStateDataSize";
    public static final String VECTOR_BATCH_SIZE = "vectorBatchSize";
    public static final String TOTAL_BACKEND_STATE_DATA_SIZE = "totalBackendStateDataSize";

    // Full operator name (matches native OperatorPOD::getName()); used as the native group key so
    // the gauges align with the backend increment path which keys by the untruncated name.
    private final String operatorName;
    // Truncated name used for the reported metric scope, mirroring Flink getOrAddOperator.
    private final String scopedOperatorName;
    // Scope component identifying the operator: OperatorID + truncated name, mirroring the key
    // Flink TaskMetricGroup.getOrAddOperator builds (key = operatorID + truncatedOperatorName).
    private final String operatorScopeName;
    private final OperatorID operatorID;
    private final long nativeCurrentMetricGroupRef;
    private final List<MetricCloseable> closeables = new ArrayList<>();

    public OmniOperatorStateMetricGroup(TaskMetricGroup taskMetricGroup, long nativeTaskMetricGroupRef,
            String operatorName, OperatorID operatorID) {
        super(OmniMetricHelper.getMetricRegistry(taskMetricGroup),
                createScopeComponents(taskMetricGroup, buildOperatorScopeName(operatorID, operatorName)),
                taskMetricGroup);
        this.operatorName = operatorName;
        this.scopedOperatorName = truncateOperatorName(operatorName);
        this.operatorID = operatorID;
        this.operatorScopeName = buildOperatorScopeName(operatorID, operatorName);
        // Key the native group by the full operator name so it matches the backend increment path.
        this.nativeCurrentMetricGroupRef = addOperatorStateMetricGroup(nativeTaskMetricGroupRef, operatorName);
        createOmniMetricsInstance();
    }

    // Mirrors Flink TaskMetricGroup.getOrAddOperator: truncate names longer than the limit.
    private static String truncateOperatorName(String operatorName) {
        if (operatorName != null && operatorName.length() > METRICS_OPERATOR_NAME_MAX_LENGTH) {
            return operatorName.substring(0, METRICS_OPERATOR_NAME_MAX_LENGTH);
        }
        return operatorName;
    }

    // Mirrors Flink TaskMetricGroup.getOrAddOperator scope key: operatorID + truncatedOperatorName.
    // Falls back to just the truncated name when no OperatorID is available (id may be unset in the
    // OmniStream task configuration).
    private static String buildOperatorScopeName(OperatorID operatorID, String operatorName) {
        String truncated = truncateOperatorName(operatorName);
//        return operatorID == null ? truncated : truncated+"_"+operatorID;
        return truncated;
    }

    private static String[] createScopeComponents(TaskMetricGroup parent, String operatorScopeName) {
        String[] parentScope = parent.getScopeComponents();
        String[] scope = new String[parentScope.length + 2];
        System.arraycopy(parentScope, 0, scope, 0, parentScope.length);
        scope[parentScope.length] = operatorScopeName;
        scope[parentScope.length + 1] = BACKEND_STATE_GROUP_NAME;
        return scope;
    }

    private void createOmniMetricsInstance() {
        // Counts (and the VectorBatch count) are int-valued SizeGauges.
        registerSizeGauge(VALUE_STATE_COUNT);
        registerSizeGauge(MAP_STATE_COUNT);
        registerSizeGauge(LIST_STATE_COUNT);
        registerSizeGauge(VECTOR_BATCH_SIZE);
        // Byte-valued data sizes use 64-bit LongSizeGauges (can exceed Integer.MAX_VALUE).
        registerLongSizeGauge(VALUE_STATE_DATA_SIZE);
        registerLongSizeGauge(MAP_STATE_DATA_SIZE);
        registerLongSizeGauge(LIST_STATE_DATA_SIZE);
        registerLongSizeGauge(VECTOR_BATCH_STATE_DATA_SIZE);
        registerLongSizeGauge(TOTAL_BACKEND_STATE_DATA_SIZE);
    }

    private void registerSizeGauge(String metricName) {
        OmniSizeGauge gauge = OmniMetricHelper.createSizeGauge(nativeCurrentMetricGroupRef, METRIC_GROUP_NAME,
                metricName);
        gauge(metricName, gauge);
        closeables.add(gauge);
    }

    private void registerLongSizeGauge(String metricName) {
        OmniLongSizeGauge gauge = OmniMetricHelper.createLongSizeGauge(nativeCurrentMetricGroupRef, METRIC_GROUP_NAME,
                metricName);
        gauge(metricName, gauge);
        closeables.add(gauge);
    }

    // Mirrors Flink InternalOperatorMetricGroup.putVariables: expose the OperatorID and operator
    // name as scope variables so reporters can resolve them.
    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put(ScopeFormat.SCOPE_OPERATOR_ID, String.valueOf(operatorID));
        variables.put(ScopeFormat.SCOPE_OPERATOR_NAME, scopedOperatorName);
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return String.join(String.valueOf(registry.getDelimiter()),
                filter.filterCharacters(operatorScopeName),
                filter.filterCharacters(BACKEND_STATE_GROUP_NAME));
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

    public static native long addOperatorStateMetricGroup(long parentGroupRef, String operatorName);
}
