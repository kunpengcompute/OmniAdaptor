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

package com.huawei.omniruntime.flink.runtime.state;

import com.huawei.omniruntime.flink.runtime.api.graph.json.TaskStateSnapshotDeser;
import com.huawei.omniruntime.flink.runtime.metrics.exception.GeneralRuntimeException;

import com.huawei.omniruntime.flink.runtime.taskmanager.OmniTask;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.TaskLocalStateStore;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * TaskStateManagerWrapper
 *
 * @since 2025-08-08
 */
public class TaskStateManagerWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(TaskStateManagerWrapper.class);

    private TaskStateManager taskStateManager;

    private OmniTask omniTask;

    public TaskStateManagerWrapper(TaskStateManager taskStateManager) {
        this.taskStateManager = taskStateManager;
    }

    // This method is invoked by cpp TaskStateManagerBridge through jni

    /**
     * reportTaskStateSnapshots
     *
     * @param checkpointMetaDataJson checkpointMetaDataJson
     * @param checkpointMetricsJson  checkpointMetricsJson
     * @param acknowledgedStateJson  acknowledgedStateJson
     * @param localStateJson         localStateJson
     * @throws FlinkRuntimeException FlinkRuntimeException
     */
    public void reportTaskStateSnapshots(
            String checkpointMetaDataJson,
            String checkpointMetricsJson,
            String acknowledgedStateJson,
            String localStateJson) throws FlinkRuntimeException {
        CheckpointMetaData checkpointMetaData = deserializeCheckpointMetaData(checkpointMetaDataJson);
        CheckpointMetrics checkpointMetrics = deserializeCheckpointMetrics(checkpointMetricsJson);

        LOG.debug("[OS-CP] reportTaskStateSnapshots cp={} acknowledgedStateJson(len={})={}",
                checkpointMetaData.getCheckpointId(),
                acknowledgedStateJson == null ? 0 : acknowledgedStateJson.length(),
                acknowledgedStateJson);
        LOG.debug("localStateJson {}", localStateJson);

        // temp mock impl
        TaskStateSnapshot localState;
        TaskStateSnapshot acknowledgedState;
        long checkpointId = checkpointMetaData.getCheckpointId();
        try {
            acknowledgedState = TaskStateSnapshotDeser.deserializeTaskStateSnapshot(acknowledgedStateJson,
                getOmniTask(), checkpointId);
            localState = TaskStateSnapshotDeser.deserializeTaskStateSnapshot(localStateJson, getOmniTask(),
                checkpointId);
        } catch (GeneralRuntimeException | JsonProcessingException e) {
            LOG.error("[OS-CP] reportTaskStateSnapshots cp={} deserialize failed", checkpointId, e);
            throw new FlinkRuntimeException(e);
        }
        // 关键诊断：看每个 operator 的 managedKeyedState 数量，定位 state handle 是否被 Java 端静默丢弃。
        if (acknowledgedState != null && acknowledgedState.getSubtaskStateMappings() != null) {
            for (Map.Entry<OperatorID, OperatorSubtaskState> entry : acknowledgedState.getSubtaskStateMappings()) {
                OperatorSubtaskState st = entry.getValue();
                int mks = (st == null || st.getManagedKeyedState() == null) ? 0 : st.getManagedKeyedState().size();
                int rks = (st == null || st.getRawKeyedState() == null) ? 0 : st.getRawKeyedState().size();
                int mos = (st == null || st.getManagedOperatorState() == null) ? 0 : st.getManagedOperatorState().size();
                LOG.debug("[OS-CP] reportTaskStateSnapshots cp={} operator={} managedKeyedState={} rawKeyedState={} managedOperatorState={}",
                        checkpointId, entry.getKey(), mks, rks, mos);
            }
        } else {
            LOG.warn("[OS-CP] reportTaskStateSnapshots cp={} acknowledgedState is null OR has no subtaskStateMappings", checkpointId);
        }
        taskStateManager.reportTaskStateSnapshots(checkpointMetaData, checkpointMetrics, acknowledgedState, localState);
    }

    public void setOmniTask(OmniTask omniTask) {
        this.omniTask = omniTask;
    }

    public OmniTask getOmniTask() {
        return this.omniTask;
    }

    private CheckpointMetrics deserializeCheckpointMetrics(String checkpointMetricsJson)
            throws GeneralRuntimeException {
        LOG.debug("deserialize checkpoint metrics json: {}", checkpointMetricsJson);
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(checkpointMetricsJson);

            long bytesProcessed = rootNode.get("bytesProcessedDuringAlignment").asLong();
            long bytesPersisted = rootNode.get("bytesPersistedDuringAlignment").asLong();
            long alignmentDuration = rootNode.get("alignmentDurationNanos").asLong();
            long syncDuration = rootNode.get("syncDurationMillis").asLong();
            long asyncDuration = rootNode.get("asyncDurationMillis").asLong();
            long startDelay = rootNode.get("checkpointStartDelayNanos").asLong();
            boolean unaligned = rootNode.get("unalignedCheckpoint").asBoolean();
            long bytesPersistedOfThis = rootNode.get("bytesPersistedOfThisCheckpoint").asLong();
            long totalBytesPersisted = rootNode.get("totalBytesPersisted").asLong();

            return new CheckpointMetrics(
                    bytesProcessed,
                    bytesPersisted,
                    alignmentDuration,
                    syncDuration,
                    asyncDuration,
                    startDelay,
                    unaligned,
                    bytesPersistedOfThis,
                    totalBytesPersisted
            );
        } catch (IOException e) {
            LOG.error("Failed to deserialize CheckpointMetrics from JSON", e);
            throw new GeneralRuntimeException("Could not parse CheckpointMetrics JSON", e);
        }
    }

    private CheckpointMetaData deserializeCheckpointMetaData(String checkpointMetaDataJson) {
        LOG.debug("checkpointMetaDataJson {}", checkpointMetaDataJson);
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(checkpointMetaDataJson);

            long checkpointId = rootNode.get("checkpointId").asLong();
            long timestamp = rootNode.get("timestamp").asLong();
            long receiveTimestamp = rootNode.get("receiveTimestamp").asLong();

            return new CheckpointMetaData(checkpointId, timestamp, receiveTimestamp);
        } catch (IOException e) {
            LOG.error("Failed to deserialize CheckpointMetaData from JSON", e);
            throw new GeneralRuntimeException("Could not parse CheckpointMetaData JSON", e);
        }
    }

    private void notifyCheckpointAborted(String checkpointidStr) throws Exception {
        long checkpointId = Long.parseLong(checkpointidStr);
        taskStateManager.notifyCheckpointAborted(checkpointId);
    }
    private void notifyCheckpointComplete(String checkpointidStr) throws Exception {
        long checkpointId = Long.parseLong(checkpointidStr);
        taskStateManager.notifyCheckpointComplete(checkpointId);
    }

    public String retrieveLocalState(long restoreCheckpointId){
        String snapshotStr = "";
        LOG.info("retrieveLocalState adaptor, checkpointId:{}",restoreCheckpointId);
        try {
            Class clazz = taskStateManager.getClass();
            Field field = clazz.getDeclaredField("localStateStore");
            field.setAccessible(true);
            TaskLocalStateStore localStateStore = (TaskLocalStateStore) field.get(taskStateManager);
            TaskStateSnapshot taskStateSnapshot = null;

            taskStateSnapshot = localStateStore.retrieveLocalState(restoreCheckpointId);

            if(taskStateSnapshot == null){
                LOG.error("get snapshot failed");
                return "NULL";
            }
            
            snapshotStr = TaskStateSnapshotDeser.serializeTaskStateSnapshot(taskStateSnapshot);
            LOG.info("Successfully retrieved snapshot for checkpointId: {}, snapshot size: {},snapshot str:{}",
                    restoreCheckpointId, snapshotStr.length(), snapshotStr);
        }catch (Exception e){
            LOG.error("Failed to get local snapshot!", e);
            return "ERROR";
        }
        return snapshotStr;
    }

}
