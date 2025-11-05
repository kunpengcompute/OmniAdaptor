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

package com.huawei.omniruntime.flink.runtime.api.graph.json;

import com.huawei.omniruntime.flink.runtime.metrics.exception.GeneralRuntimeException;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Objects;

/**
 * TaskStateSnapshotDeser
 *
 * @since 2025-08-08
 */
public class TaskStateSnapshotDeser {
    private static KeyGroupRange parseKeyGroupRange(JsonNode keyGroupNode) {
        int start = keyGroupNode.get("startKeyGroup").asInt();
        int end = keyGroupNode.get("endKeyGroup").asInt();
        return new KeyGroupRange(start, end);
    }

    public static StreamStateHandle parseStreamStateHandle(JsonNode handleNode) {
        if (handleNode == null || handleNode.isNull()) {
            return null;
        }

        String type = null;
        if (handleNode.has("@class")) {
            String cls = handleNode.get("@class").asText();
            type = cls.substring(cls.lastIndexOf('.') + 1);
        } else if (handleNode.has("stateHandleName")) {
            type = handleNode.get("stateHandleName").asText();
        } else {
            type = null;
        }
        long stateSize = handleNode.get("stateSize").asLong();
        String filePath;
        switch (type) {
            case "FileStateHandle":
                filePath = handleNode.get("filePath").asText();
                return new FileStateHandle(new Path(filePath), stateSize);
            case "RelativeFileStateHandle":
                String relativePath = handleNode.get("relativePath").asText();
                filePath = handleNode.get("filePath").asText();
                return new RelativeFileStateHandle(new Path(filePath), relativePath, stateSize);
            case "ByteStreamStateHandle":
                String handleName = handleNode.get("handleName").asText();
                byte[] data = Base64.getDecoder().decode(handleNode.get("data").asText());
                return new ByteStreamStateHandle(handleName, data);
            case "PlaceholderStreamStateHandle":
                JsonNode physicalID = handleNode.get("physicalID");
                if (physicalID != null) {
                    PhysicalStateHandleID pid =  new PhysicalStateHandleID(physicalID.get("keyString").asText());
                    return new PlaceholderStreamStateHandle(pid, stateSize);
                } else {
                    throw new IllegalArgumentException("Invalid json format of PlaceholderStreamStateHandle.");
                }
            default:
                throw new IllegalArgumentException("Unknown StreamStateHandle type: " + type);
        }
    }

    private static List<IncrementalKeyedStateHandle.HandleAndLocalPath> parseHandleAndLocalPathList(JsonNode listNode) {
        List<IncrementalKeyedStateHandle.HandleAndLocalPath> list = new ArrayList<>();
        if (listNode != null && listNode.isArray()) {
            for (JsonNode itemNode : listNode) {
                String localPath = itemNode.get("localPath").asText();
                StreamStateHandle handle = parseStreamStateHandle(itemNode.get("handle"));
                list.add(IncrementalKeyedStateHandle.HandleAndLocalPath.of(handle, localPath));
            }
        }
        return list;
    }

    /**
     * createTaskStateSnapshotInstance
     *
     * @param subtaskStates subtaskStates
     * @param isTaskDeployedAsFinished isTaskDeployedAsFinished
     * @param isTaskFinished isTaskFinished
     * @return TaskStateSnapshot
     */
    public static TaskStateSnapshot createTaskStateSnapshotInstance(
            Map<OperatorID, OperatorSubtaskState> subtaskStates,
            boolean isTaskDeployedAsFinished,
            boolean isTaskFinished) {
        try {
            Class<TaskStateSnapshot> clazz = TaskStateSnapshot.class;

            Constructor<TaskStateSnapshot> constructor = clazz.getDeclaredConstructor(
                    Map.class,      // for subtaskStatesByOperatorID
                    boolean.class,  // for isTaskDeployedAsFinished
                    boolean.class   // for isTaskFinished
            );

            constructor.setAccessible(true);
            return constructor.newInstance(subtaskStates, isTaskDeployedAsFinished, isTaskFinished);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                 | NoSuchMethodException e) {
            throw new GeneralRuntimeException("Failed to create TaskStateSnapshot instance via reflection", e);
        }
    }

    /**
     * deserializeTaskStateSnapshot
     *
     * @param jsonString jsonString
     * @return TaskStateSnapshot
     * @throws FlinkRuntimeException FlinkRuntimeException
     * @throws JsonProcessingException JsonProcessingException
     */
    public static TaskStateSnapshot deserializeTaskStateSnapshot(String jsonString) throws FlinkRuntimeException,
            JsonProcessingException {
        if (Objects.equals(jsonString, "")) {
            return null;
        }
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(jsonString);

        boolean isTaskDeployedAsFinished = rootNode.get("isTaskDeployedAsFinished").asBoolean();
        boolean isTaskFinished = rootNode.get("isTaskFinished").asBoolean();

        Map<OperatorID, OperatorSubtaskState> subtaskStates = new HashMap<>();
        JsonNode subtaskStatesNode = rootNode.get("subtaskStatesByOperatorID");

        Iterator<Map.Entry<String, JsonNode>> fields = subtaskStatesNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String operatorIdHex = entry.getKey();

            if ("@class".equals(operatorIdHex)) {
                continue;
            }

            OperatorID operatorID = new OperatorID(StringUtils.hexStringToByte(operatorIdHex));

            JsonNode operatorStateNode = entry.getValue();

            StateObjectCollection<KeyedStateHandle> managedKeyedState = new StateObjectCollection<>();
            JsonNode managedKeyedStateArray = operatorStateNode.get("managedKeyedState").get("stateObjects");

            if (managedKeyedStateArray.isArray()) {
                parseManagedKeyedStateArray(managedKeyedStateArray, managedKeyedState);
            }
            OperatorSubtaskState.Builder builder = OperatorSubtaskState.builder();

            builder.setManagedKeyedState(managedKeyedState);

            // Empty collections for all other states
            builder.setRawKeyedState(StateObjectCollection.empty());
            builder.setManagedOperatorState(StateObjectCollection.empty());
            builder.setRawOperatorState(StateObjectCollection.empty());
            builder.setInputChannelState(StateObjectCollection.empty());
            builder.setResultSubpartitionState(StateObjectCollection.empty());
            builder.setInputRescalingDescriptor(InflightDataRescalingDescriptor.NO_RESCALE);
            builder.setOutputRescalingDescriptor(InflightDataRescalingDescriptor.NO_RESCALE);
            OperatorSubtaskState operatorSubtaskState = builder.build();

            subtaskStates.put(operatorID, operatorSubtaskState);
        }

        return createTaskStateSnapshotInstance(subtaskStates, isTaskDeployedAsFinished, isTaskFinished);
    }

    private static void parseManagedKeyedStateArray(JsonNode managedKeyedStateArray,
                                                    StateObjectCollection<KeyedStateHandle> managedKeyedState) {
        for (JsonNode handleNode : managedKeyedStateArray) {
            String handleType = handleNode.get("stateHandleName").asText();
            if ("IncrementalLocalKeyedStateHandle".equals(handleType)) {
                IncrementalLocalKeyedStateHandle flinkHandle = getIncrementalLocalKeyedStateHandle(handleNode);
                managedKeyedState.add(flinkHandle);
            } else if ("IncrementalRemoteKeyedStateHandle".equals(handleType)) {
                UUID backendIdentifier = UUID.fromString(handleNode.get("backendIdentifier").asText());
                long checkpointId = handleNode.get("checkpointId").asLong();
                long persistedSize = handleNode.get("persistedSizeOfThisCheckpoint").asLong();
                KeyGroupRange keyGroupRange = parseKeyGroupRange(handleNode.get("keyGroupRange"));
                StateHandleID stateHandleId = new StateHandleID(
                        handleNode.get("stateHandleId").get("keyString").asText()
                );
                StreamStateHandle metaStateHandle = parseStreamStateHandle(handleNode.get("metaStateHandle"));
                List<IncrementalKeyedStateHandle.HandleAndLocalPath> privateState =
                        parseHandleAndLocalPathList(handleNode.get("privateState"));
                List<IncrementalKeyedStateHandle.HandleAndLocalPath> sharedState =
                        parseHandleAndLocalPathList(handleNode.get("sharedState"));

                IncrementalRemoteKeyedStateHandle flinkHandle = IncrementalRemoteKeyedStateHandle.restore(
                        backendIdentifier,
                        keyGroupRange,
                        checkpointId,
                        sharedState,
                        privateState,
                        metaStateHandle,
                        persistedSize,
                        stateHandleId
                );
                managedKeyedState.add(flinkHandle);
            }
        }
    }

    private static IncrementalLocalKeyedStateHandle getIncrementalLocalKeyedStateHandle(JsonNode handleNode) {
        UUID backendIdentifier = UUID.fromString(handleNode.get("backendIdentifier").asText());
        long checkpointId = handleNode.get("checkpointId").asLong();
        KeyGroupRange keyGroupRange = parseKeyGroupRange(handleNode.get("keyGroupRange"));

        DirectoryStateHandle directoryStateHandle = new DirectoryStateHandle(
                Paths.get(handleNode.get("directoryStateHandle").get("directory").asText())
        );
        StreamStateHandle metaDataState = parseStreamStateHandle(handleNode.get("metaDataState"));
        List<IncrementalKeyedStateHandle.HandleAndLocalPath> sharedState =
                parseHandleAndLocalPathList(handleNode.get("sharedState"));

        IncrementalLocalKeyedStateHandle flinkHandle = new IncrementalLocalKeyedStateHandle(
                backendIdentifier,
                checkpointId,
                directoryStateHandle,
                keyGroupRange,
                metaDataState,
                sharedState
        );
        return flinkHandle;
    }
}
