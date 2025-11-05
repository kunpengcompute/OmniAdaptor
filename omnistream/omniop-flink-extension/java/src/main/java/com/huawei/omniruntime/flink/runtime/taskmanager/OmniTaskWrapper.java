/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.taskmanager;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.api.graph.json.TaskStateSnapshotDeser;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.taskmanager.RuntimeEnvironment;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class OmniTaskWrapper {
    OmniTask omniTask;

    public OmniTaskWrapper(OmniTask omniTask) {
        this.omniTask = omniTask;
    }

    // this function is used for C++ side jobobject call

    private void declineCheckpoint(String checkpointID, String failureReason,String exception) {
        long checkpointid=deserilizedCheckpointID(checkpointID);
        CheckpointFailureReason failure=deserilizedfailureReason(failureReason);
        Throwable failureCause =deserilizedexception(exception);
        omniTask.declineCheckpoint(checkpointid,failure,failureCause);
    }

    private long deserilizedCheckpointID(String checkpointID){
        return Long.parseLong(checkpointID);
    }

    private CheckpointFailureReason deserilizedfailureReason(String failureReasonJson) {
        try {
            switch (failureReasonJson) {
                case "CHECKPOINT_DECLINED":
                    return CheckpointFailureReason.CHECKPOINT_DECLINED;
                case "CHECKPOINT_DECLINED_SUBSUMED":
                    return CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED;
                case "CHECKPOINT_DECLINED_TASK_NOT_READY":
                    return CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY;
                default:
                    return CheckpointFailureReason.UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE;
            }
        } catch (IllegalArgumentException e) {
            return CheckpointFailureReason.TASK_CHECKPOINT_FAILURE;
        }
    }

    private Throwable deserilizedexception(String exceptionString) {
        if(exceptionString=="nullptr"){
            return null;
        }
        String errorCode = null;
        String reason = null;
        String stack = null;

        String[] lines = exceptionString.split("\\n");
        for (String line : lines) {
            if (line.startsWith("Error Code:")) {
                errorCode = line.substring("Error Code:".length()).trim();
            } else if (line.startsWith("Reason:")) {
                reason = line.substring("Reason:".length()).trim();
            } else if (line.startsWith("Stack:")) {
                stack = line.substring("Stack:".length()).trim();
            }
        }
        String msg = "[ErrorCode=" + errorCode + "] " + reason + "\nStack: " + stack;
        return new RuntimeException(msg);
    }

    public SnapshotResult<StreamStateHandle> materializeMetaData(long checkpointId, String stateMetaInfoSnapshotsJson) throws IOException {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        List<Map<String, Object>> stateMetaInfoMaps =
                objectMapper.readValue(stateMetaInfoSnapshotsJson, new TypeReference<List<Map<String, Object>>>() {});

        List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>(stateMetaInfoMaps.size());
        for (Map<String, Object> metaInfo : stateMetaInfoMaps) {
            String name = (String) metaInfo.get("name");
            int typeCode = (Integer) metaInfo.get("backendStateType");
            Map<String, String> options = (Map<String, String>) metaInfo.get("options");

            stateMetaInfoSnapshots.add(new StateMetaInfoSnapshot(
                    name,
                    StateMetaInfoSnapshot.BackendStateType.byCode(typeCode),
                    options,
                    Collections.emptyMap(),   // serializerSnapshots
                    Collections.emptyMap())); // serializers
        }

        try {
            return omniTask.materializeMetaData(checkpointId, stateMetaInfoSnapshots);
        } catch (Exception e) {
            throw new IOException("Failed to materialize metadata", e);
        }
    }

    public List<HandleAndLocalPath> uploadFilesToCheckpointFs(String pathsJson,
                                                              int numberOfSnapshottingThreads) throws IOException {
        final ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        final List<String> pathStrs = objectMapper.readValue(pathsJson, new TypeReference<List<String>>() {});
        final List<java.nio.file.Path> paths = pathStrs.stream()
                                                    .map(java.nio.file.Paths::get)
                                                    .collect(Collectors.toList());

        try {
            List<HandleAndLocalPath> handles = omniTask.uploadFilesToCheckpointFs(paths, numberOfSnapshottingThreads);

            if (handles == null) {
                return new ArrayList<>();
            }
        
            return handles;
        } catch (Exception e) {
            throw new IOException("Failed to upload files to checkpointFs", e);
        }
    }

    private IncrementalLocalKeyedStateHandle deserializeIncrementalLocalKeyedStateHandle(String metaStateHandleStr) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            
            JsonNode rootNode = mapper.readTree(metaStateHandleStr);
            UUID backendIdentifier = UUID.fromString(rootNode.get("backendIdentifier").asText());
            long checkpointId = rootNode.get("checkpointId").asLong();
            KeyGroupRange keyGroupRange = JsonHelper.fromJson(rootNode.get("keyGroupRange").toString(), KeyGroupRange.class);
            
            JsonNode directoryStateHandleNode = rootNode.get("directoryStateHandle");
            java.nio.file.Path directoryPath = java.nio.file.Paths.get(directoryStateHandleNode.get("directoryString").asText());
            DirectoryStateHandle directoryStateHandle = new DirectoryStateHandle(directoryPath);

            StreamStateHandle metaDataState = TaskStateSnapshotDeser.parseStreamStateHandle(rootNode.get("metaDataState"));

            List<HandleAndLocalPath> sharedState = new ArrayList<>();
            JsonNode sharedStateNode = rootNode.get("sharedState").get(1);
            for (JsonNode stateNode : sharedStateNode) {
                String localPath = stateNode.get("localPath").asText();
                StreamStateHandle handle = TaskStateSnapshotDeser.parseStreamStateHandle(stateNode.get("handle"));
                sharedState.add(HandleAndLocalPath.of(handle, localPath));
            }

            return new IncrementalLocalKeyedStateHandle(
                    backendIdentifier,
                    checkpointId,
                    directoryStateHandle,
                    keyGroupRange,
                    metaDataState,
                    sharedState);
        } catch (Exception e) {
            throw new JsonHelper.JsonHelperException(
                "Error deserializing metaStateHandleStr to IncrementalLocalKeyedStateHandle: " + metaStateHandleStr, e);
        }
    }

    // This function is for C++ calling readMetaData in RocksDBIncrementalRestoreOperation
    public <K> String readMetaData(String metaStateHandleStr) throws IOException {
        // Reconstruct a IncrementalLocalStateHandle
        IncrementalLocalKeyedStateHandle localKeyedStateHandle =
                deserializeIncrementalLocalKeyedStateHandle(metaStateHandleStr);
        RuntimeEnvironment env = omniTask.checkpointingEnv;

        ClassLoader userCodeClassLoader = env.getUserCodeClassLoader().asClassLoader();

        StreamStateHandle metaStateHandle = localKeyedStateHandle.getMetaDataState();
        InputStream inputStream = null;

        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        try {
            // The readMetaData function
            inputStream = metaStateHandle.openInputStream();
            cancelStreamRegistry.registerCloseable(inputStream);
            DataInputView in = new DataInputViewStreamWrapper(inputStream);

            KeyedBackendSerializationProxy<K> serializationProxy =
                    new KeyedBackendSerializationProxy<>(userCodeClassLoader);
            serializationProxy.read(in);
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                    serializationProxy.getStateMetaInfoSnapshots();
            // Convert to a string and return to C++
            return JsonHelper.toJson(stateMetaInfoSnapshots);
        } finally {
            if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
                inputStream.close();
            }
        }
    }
}
