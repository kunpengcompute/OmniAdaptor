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

import com.huawei.omniruntime.flink.runtime.taskmanager.OmniTask;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.io.IOException;

/**
 * TaskStateSnapshotDeser
 *
 * @since 2025-08-08
 */
public class TaskStateSnapshotDeser {

    private static final Logger LOG = LoggerFactory.getLogger(TaskStateSnapshotDeser.class);

    private static final  ObjectMapper MAPPER = new ObjectMapper();

    static{
        // 配置ObjectMapper避免循环引用
        MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        MAPPER.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false);
    }

       

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
        long stateSize = handleNode.has("stateSize") ? handleNode.get("stateSize").asLong() : -1L;
        String filePath;
        switch (type) {
            case "FileStateHandle":
                filePath = handleNode.get("filePath").asText();
                return new FileStateHandle(new Path(filePath), stateSize);
            case "RelativeFileStateHandle":
                String relativePath = handleNode.get("relativePath").asText();
                if (handleNode.has("filePath")) {
                    filePath = handleNode.get("filePath").asText();
                } else {
                    filePath = handleNode.get("streamStateHandleID").get("keyString").asText();
                }
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
        JsonNode itemsNode = getStateObjectsNode(listNode);
        if (itemsNode.isArray()) {
            for (JsonNode itemNode : itemsNode) {
                if (itemNode == null || !itemNode.has("handle") || itemNode.get("handle").isNull()) {
                    continue;
                }
                String localPath = itemNode.has("localPath") ? itemNode.get("localPath").asText() : "";
                if (localPath.isEmpty()) {
                    throw new IllegalArgumentException("Incremental keyed state handle entry is missing localPath.");
                }
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
    public static TaskStateSnapshot deserializeTaskStateSnapshot(String jsonString, OmniTask omniTask, long checkpointId)
        throws FlinkRuntimeException, JsonProcessingException {
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
            JsonNode managedKeyedStateArray = getStateObjectsNode(operatorStateNode.get("managedKeyedState"));

            if (managedKeyedStateArray.isArray()) {
                parseManagedKeyedStateArray(managedKeyedStateArray, managedKeyedState);
            }

            StateObjectCollection<KeyedStateHandle> rawKeyedState = new StateObjectCollection<>();
            JsonNode rawKeyedStateArray = getStateObjectsNode(operatorStateNode.get("rawKeyedState"));

            if (rawKeyedStateArray.isArray()) {
                parseManagedKeyedStateArray(rawKeyedStateArray, rawKeyedState);
            }
            StateObjectCollection<InputChannelStateHandle> inputChannelState = new StateObjectCollection<>();
            JsonNode inputChannelStateArray = getStateObjectsNode(operatorStateNode.get("inputChannelState"));

            if (inputChannelStateArray.isArray()) {
                parseInputChannelStateArray(inputChannelStateArray, inputChannelState, omniTask, checkpointId);
            }

            StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionState = new StateObjectCollection<>();
            JsonNode resultSubpartitionStateArray = getStateObjectsNode(operatorStateNode.get("resultSubpartitionState"));

            if (resultSubpartitionStateArray.isArray()) {
                parseResultSubpartitionStateArray(resultSubpartitionStateArray, resultSubpartitionState, omniTask,
                    checkpointId);
            }
            OperatorSubtaskState.Builder builder = OperatorSubtaskState.builder();

            builder.setManagedKeyedState(managedKeyedState);
            builder.setRawKeyedState(rawKeyedState);
            builder.setInputChannelState(inputChannelState);
            builder.setResultSubpartitionState(resultSubpartitionState);
            // Empty collections for all other states
            builder.setManagedOperatorState(StateObjectCollection.empty());
            builder.setRawOperatorState(StateObjectCollection.empty());
            builder.setInputRescalingDescriptor(InflightDataRescalingDescriptor.NO_RESCALE);
            builder.setOutputRescalingDescriptor(InflightDataRescalingDescriptor.NO_RESCALE);
            OperatorSubtaskState operatorSubtaskState = builder.build();

            subtaskStates.put(operatorID, operatorSubtaskState);
        }

        return createTaskStateSnapshotInstance(subtaskStates, isTaskDeployedAsFinished, isTaskFinished);
    }

    private static JsonNode getStateObjectsNode(JsonNode collectionNode) {
        if (collectionNode == null || collectionNode.isNull()) {
            return JsonNodeFactory.instance.arrayNode();
        }
        if (collectionNode.has("stateObjects")) {
            return collectionNode.get("stateObjects");
        }
        if (collectionNode.isArray()) {
            if (collectionNode.size() == 2 && collectionNode.get(0).isTextual() && collectionNode.get(1).isArray()) {
                return collectionNode.get(1);
            }
            return collectionNode;
        }
        return JsonNodeFactory.instance.arrayNode();
    }

    private static String getStateHandleName(JsonNode handleNode) {
        if (handleNode.has("stateHandleName")) {
            return handleNode.get("stateHandleName").asText();
        }
        if (handleNode.has("@class")) {
            String className = handleNode.get("@class").asText();
            return className.substring(className.lastIndexOf('.') + 1);
        }
        return "";
    }

    private static JsonNode getFirstPresent(JsonNode node, String... fieldNames) {
        if (node == null) {
            return null;
        }
        for (String fieldName : fieldNames) {
            JsonNode child = node.get(fieldName);
            if (child != null && !child.isNull()) {
                return child;
            }
        }
        return null;
    }

    private static void parseManagedKeyedStateArray(JsonNode managedKeyedStateArray,
                                                    StateObjectCollection<KeyedStateHandle> managedKeyedState) {
        for (JsonNode handleNode : managedKeyedStateArray) {
            String handleType = getStateHandleName(handleNode);
            if ("IncrementalLocalKeyedStateHandle".equals(handleType)) {
                IncrementalLocalKeyedStateHandle flinkHandle = getIncrementalLocalKeyedStateHandle(handleNode);
                managedKeyedState.add(flinkHandle);
            } else if ("IncrementalRemoteKeyedStateHandle".equals(handleType)) {
                UUID backendIdentifier = UUID.fromString(handleNode.get("backendIdentifier").asText());
                long checkpointId = handleNode.get("checkpointId").asLong();
                JsonNode persistedSizeNode = getFirstPresent(handleNode, "persistedSizeOfThisCheckpoint", "checkpointedSize", "stateSize");
                long persistedSize = persistedSizeNode == null ? -1L : persistedSizeNode.asLong();
                KeyGroupRange keyGroupRange = parseKeyGroupRange(handleNode.get("keyGroupRange"));
                StateHandleID stateHandleId = new StateHandleID(
                        handleNode.get("stateHandleId").get("keyString").asText()
                );
                StreamStateHandle metaStateHandle = parseStreamStateHandle(
                        getFirstPresent(handleNode, "metaStateHandle", "metaDataState"));
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
            } else if ("KeyGroupsSavepointStateHandle".equals(handleType)
                    || "KeyGroupsStateHandle".equals(handleType)) {
                JsonNode groupRangeOffsetsNode = handleNode.get("groupRangeOffsets");
                JsonNode keyGroupRangeNode = groupRangeOffsetsNode != null && groupRangeOffsetsNode.has("keyGroupRange")
                        ? groupRangeOffsetsNode.get("keyGroupRange")
                        : handleNode.get("keyGroupRange");
                KeyGroupRange keyGroupRange = parseKeyGroupRange(keyGroupRangeNode);
                JsonNode offsetsNode = groupRangeOffsetsNode == null ? null : groupRangeOffsetsNode.get("offsets");

                long[] offsets = new long[keyGroupRange.getNumberOfKeyGroups()];
                if (offsetsNode != null && offsetsNode.isArray()) {
                    for (int i = 0; i < offsets.length && i < offsetsNode.size(); i++) {
                        offsets[i] = offsetsNode.get(i).asLong();
                    }
                }
                KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(keyGroupRange, offsets);
                StreamStateHandle streamStateHandle = parseStreamStateHandle(
                        getFirstPresent(handleNode, "streamStateHandle", "stateHandle", "metaDataState"));
                if ("KeyGroupsSavepointStateHandle".equals(handleType)) {
                    KeyGroupsSavepointStateHandle flinkHandle = new KeyGroupsSavepointStateHandle(keyGroupRangeOffsets, streamStateHandle);
                    managedKeyedState.add(flinkHandle);
                } else {
                    KeyGroupsStateHandle flinkHandle = new KeyGroupsStateHandle(keyGroupRangeOffsets, streamStateHandle);
                    managedKeyedState.add(flinkHandle);
                }
            }
        }
    }

    private static void parseInputChannelStateArray(JsonNode inputChannelStateArray,
        StateObjectCollection<InputChannelStateHandle> inputChannelState, OmniTask omniTask, long checkpointId) {
        if (omniTask == null) {
            LOG.error("parseResultSubpartitionStateArray failed. omniTask is null");
            return;
        }
        for (JsonNode handleNode : inputChannelStateArray) {
            String handleType = handleNode.get("@class").asText();
            if (handleType.contains("InputChannelStateHandle")) {
                int subTaskIndex = handleNode.get("subtaskIndex").asInt();
                InputChannelInfo info = null;
                StreamStateHandle delegate = null;
                JsonNode delegateNode = handleNode.get("delegate");
                String className2 = delegateNode.get("@class").asText();
                long streamPos = 0;
                if (className2.contains("ByteStreamStateHandle")) {
                    String handleName = delegateNode.get("handleName").asText();
                    String encodedData = delegateNode.get("data").asText();
                    byte[] decodedData = Base64.getDecoder().decode(encodedData);
                    delegate = new ByteStreamStateHandle(handleName, decodedData);
                } else if (className2.contains("FileStateHandle")) {
                    java.nio.file.Path filePath = Paths.get(delegateNode.get("filePath").asText());
                    try {
                        delegate = uploadInflightFileToCheckpointFs(filePath,
                            omniTask.getCheckpointStreamFactory(checkpointId), streamPos);
                    } catch (Exception e) {
                        LOG.error("uploadFilesToCheckpointFs failed. filePath: {}", filePath);
                        throw new FlinkRuntimeException(e);
                    }
                } else {
                    LOG.error("Not support for StreamStateHandle type: " + className2);
                    throw new FlinkRuntimeException("Not support for StreamStateHandle type: " + className2);
                }
                JsonNode infoNode = handleNode.get("info");
                String className3 = infoNode.get("@class").asText();
                if (className3.contains("InputChannelInfo")) {
                    int partitionIdx = infoNode.get("gateIdx").asInt();
                    int inputChannelIdx = infoNode.get("inputChannelIdx").asInt();
                    info = new InputChannelInfo(partitionIdx, inputChannelIdx);
                }
                JsonNode offsetsNode = handleNode.get("offsets");
                List<Long> offsets = new ArrayList<>();
                if (offsetsNode.isArray() && offsetsNode.size() == 2) {
                    JsonNode listNode = offsetsNode.get(1);
                    if (listNode.isArray()) {
                        for (JsonNode element : listNode) {
                            offsets.add(element.asLong() + streamPos);
                        }
                    }
                }
                int size = handleNode.get("size").asInt();
                AbstractChannelStateHandle.StateContentMetaInfo metaInfo = new AbstractChannelStateHandle.StateContentMetaInfo(offsets, size);
                inputChannelState.add(new InputChannelStateHandle(subTaskIndex, info, delegate, metaInfo));
            } else {
                LOG.error("State handle JSON is error. handleType: " + handleType);
                throw new FlinkRuntimeException("handleType is failed. handleType: " + handleType);
            }
        }
    }

    private static void parseResultSubpartitionStateArray(JsonNode resultSubpartitionStateArray,
        StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionState, OmniTask omniTask,
        long checkpointId) {
        if (omniTask == null) {
            LOG.error("parseResultSubpartitionStateArray failed. omniTask is null");
            return;
        }
        for (JsonNode handleNode : resultSubpartitionStateArray) {
            String handleType = handleNode.get("@class").asText();
            if (handleType.contains("ResultSubpartitionStateHandle")) {
                int subTaskIndex = handleNode.get("subtaskIndex").asInt();
                ResultSubpartitionInfo info = null;
                StreamStateHandle delegate = null;
                JsonNode delegateNode = handleNode.get("delegate");
                String className2 = delegateNode.get("@class").asText();
                long streamPos = 0;
                if (className2.contains("ByteStreamStateHandle")) {
                    String handleName = delegateNode.get("handleName").asText();
                    String encodedData = delegateNode.get("data").asText();
                    byte[] decodedData = Base64.getDecoder().decode(encodedData);
                    delegate = new ByteStreamStateHandle(handleName, decodedData);
                } else if (className2.contains("FileStateHandle")) {
                    java.nio.file.Path filePath = Paths.get(delegateNode.get("filePath").asText());
                    try {
                        delegate = uploadInflightFileToCheckpointFs(filePath,
                            omniTask.getCheckpointStreamFactory(checkpointId), streamPos);
                    } catch (Exception e) {
                        LOG.error("uploadFilesToCheckpointFs failed. filePath: {}", filePath);
                        throw new FlinkRuntimeException(e);
                    }
                } else {
                    LOG.error("Not support for StreamStateHandle type: " + className2);
                    throw new FlinkRuntimeException("Not support for StreamStateHandle type: " + className2);
                }
                JsonNode infoNode = handleNode.get("info");
                String className3 = infoNode.get("@class").asText();
                if (className3.contains("ResultSubpartitionInfo")) {
                    int partitionIdx = infoNode.get("partitionIdx").asInt();
                    int subPartitionIdx = infoNode.get("subPartitionIdx").asInt();
                    info = new ResultSubpartitionInfo(partitionIdx, subPartitionIdx);
                }
                JsonNode offsetsNode = handleNode.get("offsets");
                List<Long> offsets = new ArrayList<>();
                if (offsetsNode.isArray() && offsetsNode.size() == 2) {
                    JsonNode listNode = offsetsNode.get(1);
                    if (listNode.isArray()) {
                        for (JsonNode element : listNode) {
                            offsets.add(element.asLong() + streamPos);
                        }
                    }
                }
                int size = handleNode.get("size").asInt();
                AbstractChannelStateHandle.StateContentMetaInfo metaInfo = new AbstractChannelStateHandle.StateContentMetaInfo(offsets, size);
                resultSubpartitionState.add(new ResultSubpartitionStateHandle(subTaskIndex, info, delegate, metaInfo));
            } else {
                LOG.error("State handle JSON is error. handleType: " + handleType);
                throw new FlinkRuntimeException("handleType is failed. handleType: " + handleType);
            }
        }
    }

    public static StreamStateHandle uploadInflightFileToCheckpointFs(java.nio.file.Path path,
        CheckpointStreamFactory checkpointStreamFactory, long streamPos) throws Exception {
        if (checkpointStreamFactory == null) {
            throw new IllegalStateException("CheckpointStreamFactory is not initialized.");
        }

        final CloseableRegistry snapshotCloseableRegistry = new CloseableRegistry();
        final CloseableRegistry tmpResourcesRegistry = new CloseableRegistry();
        final CheckpointedStateScope stateScope = CheckpointedStateScope.EXCLUSIVE;

        StreamStateHandle handle = null;
        try {
            if (path == null) {
                return null;
            }
            handle = uploadLocalFileToCheckpointFs(path, checkpointStreamFactory, stateScope,
                    snapshotCloseableRegistry, tmpResourcesRegistry, streamPos);
            LOG.info("Checkpoint file uploaded");
        } catch (Throwable t) {
            tmpResourcesRegistry.close();
            LOG.info("Error closing registry", t);
        } finally {
            snapshotCloseableRegistry.close();
        }
        return handle;
    }

    private static StreamStateHandle uploadLocalFileToCheckpointFs(java.nio.file.Path filePath,
        CheckpointStreamFactory checkpointStreamFactory, CheckpointedStateScope stateScope,
        CloseableRegistry closeableRegistry, CloseableRegistry tmpResourcesRegistry, long streamPos) throws IOException {
        InputStream inputStream = null;
        CheckpointStateOutputStream outputStream = null;

        try {
            byte[] buffer = new byte[16384];
            inputStream = Files.newInputStream(filePath);
            closeableRegistry.registerCloseable(inputStream);
            outputStream = checkpointStreamFactory.createCheckpointStateOutputStream(stateScope);
            closeableRegistry.registerCloseable(outputStream);
            streamPos = outputStream.getPos();
            while(true) {
                int numBytes = inputStream.read(buffer);
                if (numBytes == -1) {
                    StreamStateHandle result;
                    if (closeableRegistry.unregisterCloseable(outputStream)) {
                        result = outputStream.closeAndGetHandle();
                        outputStream = null;
                    } else {
                        result = null;
                    }

                    tmpResourcesRegistry.registerCloseable(() -> {
                        StateUtil.discardStateObjectQuietly(result);
                    });
                    return result;
                }

                outputStream.write(buffer, 0, numBytes);
            }
        } finally {
            if (closeableRegistry.unregisterCloseable(inputStream)) {
                IOUtils.closeQuietly(inputStream);
            }

            if (closeableRegistry.unregisterCloseable(outputStream)) {
                IOUtils.closeQuietly(outputStream);
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

    public static String serializeTaskStateSnapshot(TaskStateSnapshot snapshot) throws IOException {
        if (snapshot == null) {
            LOG.warn("snapshot is null!");
            return "";
        }
 

        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode rootNode = factory.objectNode();

        // 1. 序列化基本字段
        rootNode.put("isTaskDeployedAsFinished", snapshot.isTaskDeployedAsFinished());
        rootNode.put("isTaskFinished", snapshot.isTaskFinished());

        // 2. 序列化subtaskStatesByOperatorID
        Set<Map.Entry<OperatorID, OperatorSubtaskState>> subtaskStates = snapshot.getSubtaskStateMappings();
        ObjectNode subtaskStatesNode = factory.objectNode();

        for (Map.Entry<OperatorID, OperatorSubtaskState> entry : subtaskStates) {
            OperatorID operatorID = entry.getKey();
            OperatorSubtaskState operatorSubtaskState = entry.getValue();

            // 将OperatorID转换为十六进制字符串作为key
            String operatorIdHex = StringUtils.byteToHexString(operatorID.getBytes());
            ObjectNode operatorStateNode = serializeOperatorSubtaskState(operatorSubtaskState);

            subtaskStatesNode.set(operatorIdHex, operatorStateNode);
        }

        rootNode.set("subtaskStatesByOperatorID", subtaskStatesNode);

        return MAPPER.writeValueAsString(rootNode);
    }

    private static ObjectNode serializeOperatorSubtaskState(OperatorSubtaskState state) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode operatorStateNode = factory.objectNode();

        // 序列化managedKeyedState
        StateObjectCollection<KeyedStateHandle> managedKeyedState = state.getManagedKeyedState();
        ArrayNode managedKeyedStateNode = serializeStateObjectCollection(managedKeyedState);
        operatorStateNode.set("managedKeyedState", managedKeyedStateNode);

        // 序列化rawKeyedState。RocksDB HEAP PQ checkpoint 的 timer 原始状态就在这里。
        StateObjectCollection<KeyedStateHandle> rawKeyedState = state.getRawKeyedState();
        ArrayNode rawKeyedStateNode = serializeStateObjectCollection(rawKeyedState);
        operatorStateNode.set("rawKeyedState", rawKeyedStateNode);

        // 序列化inputChannelState (空集合也要序列化，C++端需要该字段)
        ArrayNode inputChannelStateNode = factory.arrayNode();
        inputChannelStateNode.add("org.apache.flink.runtime.checkpoint.StateObjectCollection");
        inputChannelStateNode.add(factory.arrayNode());
        operatorStateNode.set("inputChannelState", inputChannelStateNode);

        // 序列化resultSubpartitionState
        ArrayNode resultSubpartitionStateNode = factory.arrayNode();
        resultSubpartitionStateNode.add("org.apache.flink.runtime.checkpoint.StateObjectCollection");
        resultSubpartitionStateNode.add(factory.arrayNode());
        operatorStateNode.set("resultSubpartitionState", resultSubpartitionStateNode);

        // 序列化inputRescalingDescriptor
        operatorStateNode.set("inputRescalingDescriptor", serializeNoRescaleDescriptor());

        // 序列化outputRescalingDescriptor
        operatorStateNode.set("outputRescalingDescriptor", serializeNoRescaleDescriptor());

        return operatorStateNode;
    }

    private static ObjectNode serializeNoRescaleDescriptor() {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode node = factory.objectNode();
        node.put("@class", "org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor");
        ArrayNode descriptors = factory.arrayNode();
        descriptors.add("[Lorg.apache.flink.runtime.checkpoint.InflightDataGateOrPartitionRescalingDescriptor;");
        descriptors.add(factory.arrayNode());
        node.set("gateOrPartitionDescriptors", descriptors);
        return node;
    }

    private static ArrayNode serializeStateObjectCollection(StateObjectCollection<KeyedStateHandle> collection) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ArrayNode arrayNode = factory.arrayNode();
        arrayNode.add("org.apache.flink.runtime.checkpoint.StateObjectCollection");
        ArrayNode stateObjectsArray = factory.arrayNode();
        for (Object stateHandle : collection) {
            if (stateHandle instanceof KeyedStateHandle) {
                ObjectNode handleNode = serializeKeyedStateHandle((KeyedStateHandle) stateHandle);
                stateObjectsArray.add(handleNode);
            }
        }
        arrayNode.add(stateObjectsArray);
        return arrayNode;
    }

    private static ObjectNode serializeKeyedStateHandle(KeyedStateHandle keyedStateHandle) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode handleNode = factory.objectNode();

        if (keyedStateHandle instanceof IncrementalLocalKeyedStateHandle) {
            serializeIncrementalLocalKeyedStateHandle((IncrementalLocalKeyedStateHandle) keyedStateHandle, handleNode);
        } else if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
            serializeIncrementalRemoteKeyedStateHandle((IncrementalRemoteKeyedStateHandle) keyedStateHandle, handleNode);
        } else if (keyedStateHandle instanceof DirectoryKeyedStateHandle) {
            serializeDirectoryKeyedStateHandle((DirectoryKeyedStateHandle) keyedStateHandle, handleNode);
        } else if (keyedStateHandle instanceof KeyGroupsSavepointStateHandle) {
            serializeKeyGroupsStateHandle((KeyGroupsSavepointStateHandle) keyedStateHandle,
                "KeyGroupsSavepointStateHandle", handleNode);
        } else if (keyedStateHandle instanceof KeyGroupsStateHandle) {
            serializeKeyGroupsStateHandle((KeyGroupsStateHandle) keyedStateHandle,
                "KeyGroupsStateHandle", handleNode);
        } else {
            LOG.warn("Unsupported KeyedStateHandle type: {}", keyedStateHandle.getClass().getName());
        }

        return handleNode;
    }

    private static void serializeKeyGroupsStateHandle(KeyGroupsStateHandle handle, String stateHandleName,
                                                       ObjectNode handleNode) {
        handleNode.put("@class", stateHandleName);
        handleNode.put("stateHandleName", stateHandleName);

        // 序列化 keyGroupRange
        KeyGroupRange keyGroupRange = handle.getKeyGroupRange();
        handleNode.set("keyGroupRange", serializeKeyGroupRange(keyGroupRange));

        // 序列化 groupRangeOffsets
        KeyGroupRangeOffsets groupRangeOffsets = handle.getGroupRangeOffsets();
        ObjectNode groupRangeOffsetsNode = JsonNodeFactory.instance.objectNode();
        groupRangeOffsetsNode.set("keyGroupRange", serializeKeyGroupRange(groupRangeOffsets.getKeyGroupRange()));
        ArrayNode offsetsArray = JsonNodeFactory.instance.arrayNode();
        for (int kg = keyGroupRange.getStartKeyGroup(); kg <= keyGroupRange.getEndKeyGroup(); kg++) {
            offsetsArray.add(groupRangeOffsets.getKeyGroupOffset(kg));
        }
        groupRangeOffsetsNode.set("offsets", offsetsArray);
        handleNode.set("groupRangeOffsets", groupRangeOffsetsNode);

        // 序列化 stateHandle (StreamStateHandle)
        StreamStateHandle streamStateHandle = handle.getDelegateStateHandle();
        if (streamStateHandle != null) {
            ObjectNode streamStateHandleNode = serializeStreamStateHandle(streamStateHandle);
            handleNode.set("streamStateHandle", streamStateHandleNode);
            // Keep the old field name for compatibility with older C++ deserializers.
            handleNode.set("stateHandle", streamStateHandleNode);
        }

        // 序列化 stateHandleId
        ObjectNode stateHandleIdNode = JsonNodeFactory.instance.objectNode();
        stateHandleIdNode.put("keyString", handle.getStateHandleId().getKeyString());
        handleNode.set("stateHandleId", stateHandleIdNode);
    }

    public static void serializeDirectoryKeyedStateHandle(DirectoryKeyedStateHandle handle, ObjectNode handleNode) {
        // 1. 序列化directoryStateHandle
        DirectoryStateHandle directoryStateHandle = handle.getDirectoryStateHandle();
        handleNode.put("directoryStateHandle", directoryStateHandle.getDirectory().toString());

        // 2. 序列化keyGroupRange
        KeyGroupRange keyGroupRange = handle.getKeyGroupRange();
        handleNode.set("keyGroupRange", serializeKeyGroupRange(keyGroupRange));
    }

    private static void serializeIncrementalRemoteKeyedStateHandle(
            IncrementalRemoteKeyedStateHandle handle, ObjectNode handleNode) {
        handleNode.put("@class","IncrementalRemoteKeyedStateHandle");
        handleNode.put("stateHandleName", "IncrementalRemoteKeyedStateHandle");
        handleNode.put("backendIdentifier", handle.getBackendIdentifier().toString());
        handleNode.put("checkpointId", handle.getCheckpointId());
        handleNode.put("persistedSizeOfThisCheckpoint", handle.getCheckpointedSize());

        // 序列化KeyGroupRange
        KeyGroupRange keyGroupRange = handle.getKeyGroupRange();
        handleNode.set("keyGroupRange", serializeKeyGroupRange(keyGroupRange));

        // 序列化StateHandleID
        StateHandleID stateHandleId = handle.getStateHandleId();
        ObjectNode stateHandleIdNode = JsonNodeFactory.instance.objectNode();
        stateHandleIdNode.put("keyString", stateHandleId.getKeyString());
        handleNode.set("stateHandleId", stateHandleIdNode);

        // 序列化metaStateHandle
        StreamStateHandle metaStateHandle = handle.getMetaStateHandle();
        if (metaStateHandle != null) {
            handleNode.set("metaStateHandle", serializeStreamStateHandle(metaStateHandle));
        }

        // 序列化privateState和sharedState
        List<IncrementalKeyedStateHandle.HandleAndLocalPath> privateState = handle.getPrivateState();
        if (privateState != null && !privateState.isEmpty()) {
            JsonNodeFactory factory = JsonNodeFactory.instance;
            ArrayNode arrayNode = factory.arrayNode();
            for (IncrementalKeyedStateHandle.HandleAndLocalPath handleAndLocalPath : privateState) {
                ObjectNode node = serializeHandleAndLocalPath(handleAndLocalPath);
                arrayNode.add(node);
            }
            handleNode.set("privateState", arrayNode);
        }

        List<IncrementalKeyedStateHandle.HandleAndLocalPath> sharedState = handle.getSharedState();
        if (sharedState != null && !sharedState.isEmpty()) {
            JsonNodeFactory factory = JsonNodeFactory.instance;
            ArrayNode arrayNode = factory.arrayNode();
            for (IncrementalKeyedStateHandle.HandleAndLocalPath handleAndLocalPath : sharedState) {
                ObjectNode node = serializeHandleAndLocalPath(handleAndLocalPath);
                arrayNode.add(node);
            }
            handleNode.set("sharedState", arrayNode);
        }
    }

    private static ObjectNode serializeKeyGroupRange(KeyGroupRange keyGroupRange) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode jsonNode = factory.objectNode();

        jsonNode.put("startKeyGroup", keyGroupRange.getStartKeyGroup());
        jsonNode.put("endKeyGroup", keyGroupRange.getEndKeyGroup());

        return jsonNode;
    }

    private static void serializeIncrementalLocalKeyedStateHandle(
            IncrementalLocalKeyedStateHandle handle, ObjectNode handleNode) {
        handleNode.put("@class","IncrementalLocalKeyedStateHandle");
        handleNode.put("stateHandleName", "IncrementalLocalKeyedStateHandle");
        handleNode.put("backendIdentifier", handle.getBackendIdentifier().toString());
        handleNode.put("checkpointId", handle.getCheckpointId());

        // 序列化KeyGroupRange
        KeyGroupRange keyGroupRange = handle.getKeyGroupRange();
        handleNode.set("keyGroupRange", serializeKeyGroupRange(keyGroupRange));

        // 序列化DirectoryStateHandle
        DirectoryStateHandle directoryStateHandle = handle.getDirectoryStateHandle();
        ObjectNode directoryNode = JsonNodeFactory.instance.objectNode();
        directoryNode.put("directoryString", directoryStateHandle.getDirectory().toString());
        directoryNode.put("stateSize", directoryStateHandle.getStateSize());
        handleNode.set("directoryStateHandle", directoryNode);

        // 序列化metaDataState
        StreamStateHandle metaDataState = handle.getMetaDataState();
        if (metaDataState != null) {
            handleNode.set("metaDataState", serializeStreamStateHandle(metaDataState));
        }

        // 序列化sharedState
        List<IncrementalKeyedStateHandle.HandleAndLocalPath> sharedState = handle.getSharedStateHandles();
        if (sharedState != null && !sharedState.isEmpty()) {
            JsonNodeFactory factory = JsonNodeFactory.instance;
            ArrayNode arrayNode = factory.arrayNode();
            for (IncrementalKeyedStateHandle.HandleAndLocalPath handleAndLocalPath : sharedState) {
                ObjectNode node = serializeHandleAndLocalPath(handleAndLocalPath);
                arrayNode.add(node);
            }
            handleNode.set("sharedState", arrayNode);
        }
    }

    public static ObjectNode serializeHandleAndLocalPath(IncrementalKeyedStateHandle.HandleAndLocalPath handleAndPath) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode jsonNode = factory.objectNode();

        if (handleAndPath == null) {
            LOG.warn("handleAndPath is null!");
            return jsonNode;
        }

        // 序列化handle字段
        StreamStateHandle handle =  handleAndPath.getHandle();
        if (handle != null) {
            ObjectNode handleNode = serializeStreamStateHandle(handle);
            jsonNode.set("handle", handleNode);
        } else {
            jsonNode.set("handle", factory.nullNode());
        }

        // 序列化localPath字段
        String localPath = handleAndPath.getLocalPath();
        jsonNode.put("localPath", localPath != null ? localPath : "");
        jsonNode.put("stateSize", handleAndPath.getStateSize());

        return jsonNode;
    }

    private static ObjectNode serializeStreamStateHandle(StreamStateHandle streamStateHandle) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode handleNode = factory.objectNode();

        if (streamStateHandle instanceof RelativeFileStateHandle) {
            RelativeFileStateHandle relativeFileStateHandle = (RelativeFileStateHandle)streamStateHandle;
            handleNode.put("@class","RelativeFileStateHandle");
            PhysicalStateHandleID stateHandleId = relativeFileStateHandle.getStreamStateHandleID();
            ObjectNode stateHandleIdNode = factory.objectNode();
            stateHandleIdNode.put("keyString", stateHandleId.getKeyString());
            handleNode.set("streamStateHandleID", stateHandleIdNode);
            handleNode.put("stateSize", relativeFileStateHandle.getStateSize());
            String relativePath = relativeFileStateHandle.getRelativePath();
            handleNode.put("relativePath", relativePath);
            Path filePath = relativeFileStateHandle.getFilePath();
            handleNode.put("filePath", filePath.toString());
        } else if (streamStateHandle instanceof FileStateHandle) {
            FileStateHandle fileStateHandle = (FileStateHandle)streamStateHandle;
            handleNode.put("@class","FileStateHandle");
            PhysicalStateHandleID stateHandleId = fileStateHandle.getStreamStateHandleID();
            ObjectNode stateHandleIdNode = factory.objectNode();
            stateHandleIdNode.put("keyString", stateHandleId.getKeyString());
            handleNode.set("streamStateHandleID", stateHandleIdNode);
            handleNode.put("stateSize", fileStateHandle.getStateSize());
            Path filePath = fileStateHandle.getFilePath();
            handleNode.put("filePath", filePath.toString());
        } else if (streamStateHandle instanceof ByteStreamStateHandle) {
            ByteStreamStateHandle byteStreamStateHandle = (ByteStreamStateHandle)streamStateHandle;
            handleNode.put("@class","ByteStreamStateHandle");
            handleNode.put("handleName", byteStreamStateHandle.getHandleName());
            byte[] data = byteStreamStateHandle.getData();
            if (data != null) {
                String encodedData = Base64.getEncoder().encodeToString(data);
                handleNode.put("data", encodedData);
                handleNode.put("stateSize", byteStreamStateHandle.getStateSize());
            }
        } else if (streamStateHandle instanceof PlaceholderStreamStateHandle) {
            PlaceholderStreamStateHandle placeholderStreamStateHandle
                    = (PlaceholderStreamStateHandle) streamStateHandle;
            handleNode.put("@class", "PlaceholderStreamStateHandle");
            handleNode.put("stateSize", placeholderStreamStateHandle.getStateSize());
            ObjectNode innerNode = factory.objectNode();
            innerNode.put("keyString", placeholderStreamStateHandle.getStreamStateHandleID().getKeyString());
            handleNode.set("physicalID", innerNode);
        } else {
            throw new GeneralRuntimeException("get unsupported stream type");
        }

        return handleNode;
    }
}
