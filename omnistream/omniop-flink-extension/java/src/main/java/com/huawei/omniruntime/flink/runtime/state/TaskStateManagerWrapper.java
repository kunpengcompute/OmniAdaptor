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

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * TaskStateManagerWrapper
 *
 * @since 2025-08-08
 */
public class TaskStateManagerWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(TaskStateManagerWrapper.class);

    private TaskStateManager taskStateManager;

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

        LOG.debug("acknowledgedStateJson {}", acknowledgedStateJson);
        LOG.debug("localStateJson {}", localStateJson);

        // temp mock impl
        TaskStateSnapshot localState;
        TaskStateSnapshot acknowledgedState;
        try {
            acknowledgedState = TaskStateSnapshotDeser.deserializeTaskStateSnapshot(acknowledgedStateJson);
            localState = TaskStateSnapshotDeser.deserializeTaskStateSnapshot(localStateJson);
        } catch (GeneralRuntimeException | JsonProcessingException e) {
            throw new FlinkRuntimeException(e);
        }
        taskStateManager.reportTaskStateSnapshots(checkpointMetaData, checkpointMetrics, acknowledgedState, localState);
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

}
