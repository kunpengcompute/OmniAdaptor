/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.taskexecutor;

import com.huawei.omniruntime.flink.runtime.metrics.exception.GeneralRuntimeException;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.AcknowledgeCheckpointEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.StringUtils;

import java.io.IOException;

public class TaskOperatorGatewayWrapper {
        public TaskOperatorEventGateway taskOperatorEventGateway;
        private final ObjectMapper mapper = new ObjectMapper();

        public TaskOperatorGatewayWrapper(TaskOperatorEventGateway taskOperatorEventGateway) {
            this.taskOperatorEventGateway = taskOperatorEventGateway;
        }
        // called from cpp to java

        public void sendOperatorEventToCoordinator(String operatorStr, String eventJson) throws IOException {
                OperatorID Operatorid = deserializeOperatorid(operatorStr);
                SerializedValue<OperatorEvent> eventid = deserializeeventid(eventJson);
                taskOperatorEventGateway.sendOperatorEventToCoordinator(Operatorid, eventid);
        }
        public OperatorID deserializeOperatorid(String operatorStr) throws IOException {
            return new OperatorID(StringUtils.hexStringToByte(operatorStr));
        }

        public SerializedValue<OperatorEvent> deserializeeventid(String json) throws IOException {
                try {
                        JsonNode node = mapper.readTree(json);
                        long checkpointId = node.get("checkpointId").asLong();
                        OperatorEvent event = new AcknowledgeCheckpointEvent(checkpointId);
                        return new SerializedValue<>(event);
                } catch (IOException e) {
                        throw new GeneralRuntimeException("Could not parse CheckpointMetrics JSON", e);
                }

        }

}
