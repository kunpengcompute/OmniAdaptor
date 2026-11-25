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

package com.huawei.omniruntime.flink.runtime.taskmanager;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.api.graph.json.TaskStateSnapshotDeser;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerKey;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerKeyedStateType;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerOperatorStateMode;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerType;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.utils.OmniStateSerializerUtils;
import com.huawei.omniruntime.flink.runtime.restore.KeyGroupEntry;
import com.huawei.omniruntime.flink.runtime.restore.KeyGroupEntryWrapper;

import org.apache.flink.core.execution.SavepointFormatType;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.OmniStateSerializerHelper;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniStateMetaSerializerInfo;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProviderImpl;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.OperatorStateHandle.Mode;
import org.apache.flink.runtime.state.OperatorStateHandle.StateMetaInfo;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.OperatorBackendSerializationProxy;
import org.apache.flink.runtime.taskmanager.RuntimeEnvironment;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.FullSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.runtime.state.FullSnapshotUtil.clearMetaDataFollowsFlag;
import static org.apache.flink.runtime.state.FullSnapshotUtil.hasMetaDataFollowsFlag;

public class OmniTaskWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(OmniTaskWrapper.class);

    OmniTask omniTask;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final int MAX_KEY_GROUP_ENTRY_BATCH_SIZE = 1000;
    private static final int SINGLE_BYTE_MAX_GROUPS = 128;
    private static final int PREFIX_ONE_BYTE = 1;
    private static final int PREFIX_TWO_BYTES = 2;

    private final Map<FSDataInputStream, KeyedStateInputContext> keyedStateInputContexts =
            Collections.synchronizedMap(new IdentityHashMap<>());

    private static final Object OPERATOR_METADATA_CACHE_LOCK = new Object();
    private static final Map<ClassLoader, ConcurrentHashMap<OperatorMetadataCacheKey,
            CompletableFuture<byte[]>>> OPERATOR_METADATA_CACHE_BY_CLASSLOADER =
            new WeakHashMap<>();

    private static ConcurrentHashMap<OperatorMetadataCacheKey, CompletableFuture<byte[]>>
            getOperatorMetadataCache(ClassLoader userCodeClassLoader) {
        synchronized (OPERATOR_METADATA_CACHE_LOCK) {
            return OPERATOR_METADATA_CACHE_BY_CLASSLOADER.computeIfAbsent(
                    userCodeClassLoader,
                    ignored -> new ConcurrentHashMap<>());
        }
    }

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
        if ("nullptr".equals(exceptionString)) {
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
        // 如果是纯文本字符串（不包含 Error Code: / Reason: / Stack: 格式），
        // 直接使用原始字符串作为异常消息，保留 C++ 侧传入的真实错误原因
        if (errorCode == null && reason == null && stack == null) {
            return new RuntimeException(exceptionString);
        }
        String msg = "[ErrorCode=" + errorCode + "] " + reason + "\nStack: " + stack;
        return new RuntimeException(msg);
    }

    private CheckpointOptions parseCheckpointOptions(String checkpointOptionStr) throws Exception{
        JsonNode root = OBJECT_MAPPER.readTree(checkpointOptionStr);

        long alignedCheckpointTimeout =  root.get("alignedCheckpointTimeout").longValue();

        boolean isExactlyOnceMode = false;
        boolean isUnalignedEnabled = false;
        String alignmentType =  root.get("alignmentType").textValue();
        if (alignmentType == null || alignmentType.isEmpty()){
            throw new IllegalArgumentException("alignmentType is required");
        }
        if (alignmentType.equals(CheckpointOptions.AlignmentType.AT_LEAST_ONCE.name())) {
            isExactlyOnceMode = true;
        } else if (alignmentType.equals(CheckpointOptions.AlignmentType.UNALIGNED.name())) {
            isUnalignedEnabled = true;
        }
        JsonNode checkpointTypeNode =  root.get("checkpointType");
        if (checkpointTypeNode == null){
            throw new IllegalArgumentException("Missing required field: checkpointType");
        }
        SnapshotType type;
        String name = checkpointTypeNode.get("name").textValue();
        if (name == null){
            throw new IllegalArgumentException("Missing required field: checkpointType name");
        }
        CheckpointOptions options;
        if (name.contains("Checkpoint")){
            options = CheckpointOptions.forCheckpointWithDefaultLocation();
        } else {
            int formatType = checkpointTypeNode.get("formatType").intValue();
            SavepointFormatType sft;
            switch (formatType) {
                case 0:  sft = SavepointFormatType.CANONICAL;  break;
                case 1:  sft = SavepointFormatType.NATIVE;     break;
                case 2:  sft = SavepointFormatType.COMPATIBLE; break;  // 新增分支
                default: throw new IllegalArgumentException("Unsupported formatType: " + formatType);
            }
            if (name.equals("Savepoint")) {
                type = SavepointType.savepoint(sft);
            } else if (name.equals("Terminate Savepoint")){
                type = SavepointType.terminate(sft);
            } else {
                type = SavepointType.suspend(sft);
            }
            JsonNode targetLocationNode = root.get("targetLocation");
            if (targetLocationNode == null){
                throw new IllegalArgumentException("Missing required field: targetLocation");
            }
            String referenceBytesStr = targetLocationNode.get("referenceBytes").textValue();
            if (referenceBytesStr == null || referenceBytesStr.isEmpty()){
                throw new IllegalArgumentException("targetLocation.referenceBytes is required");
            }
            CheckpointStorageLocationReference locationReference = new CheckpointStorageLocationReference(referenceBytesStr.getBytes(StandardCharsets.UTF_8));

            options = CheckpointOptions.forConfig(
                    type,
                    locationReference,
                    isExactlyOnceMode,
                    isUnalignedEnabled,
                    alignedCheckpointTimeout);
        }
        return options;
    }

    public SnapshotResult<StreamStateHandle> materializeMetaData(long checkpointId,
                                                                 String stateMetaInfoSnapshotsJson,
                                                                 String localRecoveryConfigStr,
                                                                 String checkpointOptionStr) throws IOException {
        try {
            LOG.debug("method : materializeMetaData -> stateMetaInfoSnapshotsJson : {}", stateMetaInfoSnapshotsJson);

            List<Map<String, Object>> stateMetaInfoMaps =
                    OBJECT_MAPPER.readValue(stateMetaInfoSnapshotsJson, new TypeReference<List<Map<String, Object>>>() {
                    });

            LocalRecoveryConfig recoveryConfig = null;
            if (!"{}".equals(localRecoveryConfigStr)) {
                Map<String, Object> configMap = OBJECT_MAPPER.readValue(localRecoveryConfigStr, new TypeReference<Map<String, Object>>() {
                });
                List<String> dirs = (List<String>) configMap.get("allocationBaseDirs");
                File[] files = new File[dirs.size()];
                for (int i = 0; i < dirs.size(); i++) {
                    files[i] = new File(dirs.get(i));
                }

                String jobIdHexStr = (String) configMap.get("jobID");
                String jobVertexIdHexStr = (String) configMap.get("jobVertexID");

                JobID jobID = JobID.fromHexString(jobIdHexStr);
                JobVertexID jobVertexID = JobVertexID.fromHexString(jobVertexIdHexStr);
                int subtaskIndex = (Integer) configMap.get("subtaskIndex");
                LocalRecoveryDirectoryProvider provider = new LocalRecoveryDirectoryProviderImpl(files, jobID, jobVertexID,
                        subtaskIndex);
                recoveryConfig = new LocalRecoveryConfig(provider);
            }

            ExecutionConfig executionConfig = omniTask.getExecutionConfig();
            ClassLoader userCodeClassLoader = omniTask.getCheckpointingEnv()
                    .getUserCodeClassLoader().asClassLoader();
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>(stateMetaInfoMaps.size());
            TypeSerializer<?> keySerializer = null;
            for (Map<String, Object> metaInfo : stateMetaInfoMaps) {
                String name = (String) metaInfo.get("name");
                int typeCode = (Integer) metaInfo.get("backendStateType");

                Map<String, String> options = (Map<String, String>) metaInfo.get("options");
                String keyedStateTypeValue = options.get(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString());
                OmniSerializerKeyedStateType keyedStateType = OmniSerializerKeyedStateType.get(keyedStateTypeValue);
                if (null == keyedStateType) {
                    LOG.warn("method : materializeMetaData -> keyedStateTypeValue : {} undefined.", keyedStateTypeValue);
                } else {
                    options.put(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(), keyedStateType.getTypeName());
                }

                if (null == keySerializer) {
                    // build
                    keySerializer = OmniStateSerializerHelper.getStateBackendKeySerializer(metaInfo, executionConfig, userCodeClassLoader);
                    LOG.debug("method : materializeMetaData -> keySerializer : {}", keySerializer);
                }

                Map<String, String> serializer = JsonHelper.fromJson(metaInfo.get("serializer").toString(), HashMap.class);
                // deal
                OmniStateMetaSerializerInfo.Builder builder = OmniStateSerializerHelper.buildSerializerInfo(
                        name,
                        typeCode,
                        serializer,
                        executionConfig,
                        userCodeClassLoader);
                OmniStateMetaSerializerInfo serializerInfo = null;
                if (null != builder) {
                    builder.stateName(name);
                    builder.options(options);
                    serializerInfo = builder.build();
                }
                LOG.debug("method : materializeMetaData -> serializerInfo : {}", serializerInfo);

                stateMetaInfoSnapshots.add(new StateMetaInfoSnapshot(
                        name,
                        StateMetaInfoSnapshot.BackendStateType.byCode(typeCode),
                        options,
                        null == serializerInfo ? Collections.emptyMap() : serializerInfo.getSerializerSnapshotGroup(),
                        null == serializerInfo ? Collections.emptyMap() : serializerInfo.getSerializerGroup()));
            }
            LOG.debug("method : materializeMetaData -> stateMetaInfoSnapshots : {}", stateMetaInfoSnapshots);

            return omniTask.materializeMetaData(checkpointId, stateMetaInfoSnapshots, recoveryConfig, parseCheckpointOptions(checkpointOptionStr), keySerializer);
        } catch (Exception e) {
            LOG.error("method : materializeMetaData -> exception", e);
            throw new IOException("Failed to materialize metadata", e);
        }
    }

    public CheckpointStreamWithResultProvider acquireSavepointOutputStream(long checkpointId, String checkpointOptionStr) throws Exception {
        return omniTask.acquireSavepointOutputStream(checkpointId, parseCheckpointOptions(checkpointOptionStr));
    }

    public SnapshotResult<StreamStateHandle> closeSavepointOutputStream(CheckpointStreamWithResultProvider provider) throws Exception {
        return omniTask.closeSavepointOutputStream(provider);
    }

    public void writeSavepointOutputStream(CheckpointStreamWithResultProvider provider, byte[] chunk) throws Exception {
        omniTask.writeSavepointOutputStream(provider, chunk);
    }

    public boolean writeSavepointOutputStreamDirect(
            CheckpointStreamWithResultProvider provider, ByteBuffer chunk, int len) throws Exception {
        return omniTask.writeSavepointOutputStreamDirect(provider, chunk, len);
    }

    public void writeSavepointMetadata(CheckpointStreamWithResultProvider provider, String stateMetaInfoSnapshotsJson) throws Exception {
        try {
            LOG.debug("method : writeSavepointMetadata -> stateMetaInfoSnapshotsJson : {}", stateMetaInfoSnapshotsJson);
            List<Map<String, Object>> stateMetaInfoMaps =
                    OBJECT_MAPPER.readValue(stateMetaInfoSnapshotsJson, new TypeReference<List<Map<String, Object>>>() {
                    });

            ExecutionConfig executionConfig = omniTask.getExecutionConfig();
            ClassLoader userCodeClassLoader = omniTask.getCheckpointingEnv()
                    .getUserCodeClassLoader().asClassLoader();
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>(stateMetaInfoMaps.size());
            TypeSerializer<?> keySerializer = null;
            for (Map<String, Object> metaInfo : stateMetaInfoMaps) {
                String name = (String) metaInfo.get("name");
                int typeCode = (Integer) metaInfo.get("backendStateType");

                Map<String, String> options = (Map<String, String>) metaInfo.get("options");
                String keyedStateTypeValue = options.get(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString());
                OmniSerializerKeyedStateType keyedStateType = OmniSerializerKeyedStateType.get(keyedStateTypeValue);
                if (null == keyedStateType) {
                    LOG.warn("method : writeSavepointMetadata -> keyedStateTypeValue : {} undefined.", keyedStateTypeValue);
                } else {
                    options.put(StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(), keyedStateType.getTypeName());
                }

                if(null == keySerializer){
                    // build
                    keySerializer = OmniStateSerializerHelper.getStateBackendKeySerializer(metaInfo, executionConfig, userCodeClassLoader);
                    LOG.debug("method : writeSavepointMetadata -> keySerializer : {}", keySerializer);
                }

                Map<String, String> serializer = JsonHelper.fromJson(metaInfo.get("serializer").toString(), HashMap.class);

                // deal
                OmniStateMetaSerializerInfo.Builder builder = OmniStateSerializerHelper.buildSerializerInfo(
                        name,
                        typeCode,
                        serializer,
                        executionConfig,
                        userCodeClassLoader);
                OmniStateMetaSerializerInfo serializerInfo = null;
                if (null != builder) {
                    builder.stateName(name);
                    builder.options(options);
                    serializerInfo = builder.build();
                }
                LOG.debug("method : writeSavepointMetadata -> serializerInfo : {}", serializerInfo);

                stateMetaInfoSnapshots.add(new StateMetaInfoSnapshot(
                        name,
                        StateMetaInfoSnapshot.BackendStateType.byCode(typeCode),
                        options,
                        null == serializerInfo ? Collections.emptyMap() : serializerInfo.getSerializerSnapshotGroup(),
                        null == serializerInfo ? Collections.emptyMap() : serializerInfo.getSerializerGroup()));
            }
            LOG.debug("method : writeSavepointMetadata -> stateMetaInfoSnapshots : {}", stateMetaInfoSnapshots);

            omniTask.writeSavepointMetadata(provider, stateMetaInfoSnapshots, keySerializer);
        } catch (Exception e) {
            LOG.error("method : writeSavepointMetadata -> exception", e);
            throw new IOException("Failed to writeSavepoint metadata", e);
        }
    }

    /**
     * Returns the pre-serialized operator metadata bytes, reusing a cached
     * copy when the same metadata JSON has been seen before (same JobID + JSON content).
     *
     * <p>Multiple subtasks within a pipeline often share identical operator metadata
     * (especially for simple ListState with Long or byte[] serializers). Caching
     * avoids redundant JSON parsing, StateMetaInfoSnapshot construction, and
     * serialization per checkpoint. The cache is scoped by user-code ClassLoader
     * and uses CompletableFuture to ensure only one build per key.
     */
    private byte[] getOrBuildOperatorMetadata(
            String operatorStateMetaInfoSnapshotsJson,
            String broadcastStateMetaInfoSnapshotsJson) throws Exception {
        ClassLoader userCodeClassLoader = omniTask.getCheckpointingEnv()
                .getUserCodeClassLoader().asClassLoader();
        OperatorMetadataCacheKey key = new OperatorMetadataCacheKey(
                omniTask.getJobID(),
                operatorStateMetaInfoSnapshotsJson,
                broadcastStateMetaInfoSnapshotsJson);
        ConcurrentHashMap<OperatorMetadataCacheKey, CompletableFuture<byte[]>> cache =
                getOperatorMetadataCache(userCodeClassLoader);
        CompletableFuture<byte[]> newFuture = new CompletableFuture<>();
        CompletableFuture<byte[]> existingFuture = cache.putIfAbsent(key, newFuture);
        boolean owner = existingFuture == null;
        CompletableFuture<byte[]> future = owner ? newFuture : existingFuture;
        if (owner) {
            try {
                byte[] metadata = buildOperatorMetadata(
                        operatorStateMetaInfoSnapshotsJson,
                        broadcastStateMetaInfoSnapshotsJson);
                newFuture.complete(metadata);
                return metadata;
            } catch (Throwable t) {
                cache.remove(key, newFuture);
                newFuture.completeExceptionally(t);
                if (t instanceof Exception) {
                    throw (Exception) t;
                }
                if (t instanceof Error) {
                    throw (Error) t;
                }
                throw new IOException("Failed to build operator metadata", t);
            }
        }
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IOException("Failed to build cached operator metadata", cause);
        }
    }

    private byte[] buildOperatorMetadata(
            String operatorStateMetaInfoSnapshotsJson,
            String broadcastStateMetaInfoSnapshotsJson) throws IOException {
        List<Map<String, Object>> operatorStateMetaInfoMapList =
                JsonHelper.fromJson(operatorStateMetaInfoSnapshotsJson, new TypeReference<List<Map<String, Object>>>() {
                });
        List<Map<String, Object>> broadcastStateMetaInfoMapList =
                JsonHelper.fromJson(broadcastStateMetaInfoSnapshotsJson, new TypeReference<List<Map<String, Object>>>() {
                });

        byte[] fastMetadata = tryBuildFastOperatorMetadata(
                operatorStateMetaInfoMapList,
                broadcastStateMetaInfoMapList);
        if (fastMetadata != null) {
            return fastMetadata;
        }

        List<StateMetaInfoSnapshot> operatorStateMetaInfoSnapshotList =
                OmniStateSerializerUtils.buildStateMetaInfoSnapshot(omniTask, operatorStateMetaInfoMapList);
        List<StateMetaInfoSnapshot> broadcastStateMetaInfoSnapshotList =
                OmniStateSerializerUtils.buildStateMetaInfoSnapshot(omniTask, broadcastStateMetaInfoMapList);

        DataOutputSerializer out = new DataOutputSerializer(256);
        OperatorBackendSerializationProxy backendSerializationProxy =
                new OperatorBackendSerializationProxy(
                        operatorStateMetaInfoSnapshotList,
                        broadcastStateMetaInfoSnapshotList);
        backendSerializationProxy.write(out);

        return out.getCopyOfBuffer();
    }

    /**
     * Attempts a fast metadata build path that avoids reflection-based serializer
     * construction. Only applicable when all operator states use simple serializers
     * (Long, byte[]) with no broadcast state. Returns null when the fast path cannot
     * be taken, triggering the full build path in {@link #buildOperatorMetadata}.
     */
    private byte[] tryBuildFastOperatorMetadata(
            List<Map<String, Object>> operatorStateMetaInfoMapList,
            List<Map<String, Object>> broadcastStateMetaInfoMapList) throws IOException {
        if (operatorStateMetaInfoMapList == null
                || operatorStateMetaInfoMapList.isEmpty()
                || (broadcastStateMetaInfoMapList != null && !broadcastStateMetaInfoMapList.isEmpty())) {
            return null;
        }

        List<StateMetaInfoSnapshot> operatorStateMetaInfoSnapshotList =
                new ArrayList<>(operatorStateMetaInfoMapList.size());
        for (Map<String, Object> metaInfo : operatorStateMetaInfoMapList) {
            StateMetaInfoSnapshot snapshot = tryBuildFastOperatorStateMetaInfo(metaInfo);
            if (snapshot == null) {
                return null;
            }
            operatorStateMetaInfoSnapshotList.add(snapshot);
        }

        DataOutputSerializer out = new DataOutputSerializer(256);
        OperatorBackendSerializationProxy backendSerializationProxy =
                new OperatorBackendSerializationProxy(
                        operatorStateMetaInfoSnapshotList,
                        Collections.emptyList());
        backendSerializationProxy.write(out);

        return out.getCopyOfBuffer();
    }

    private StateMetaInfoSnapshot tryBuildFastOperatorStateMetaInfo(Map<String, Object> metaInfo) {
        if (metaInfo == null) {
            return null;
        }
        Object typeCodeObj = metaInfo.get("backendStateType");
        if (!(typeCodeObj instanceof Number)
                || StateMetaInfoSnapshot.BackendStateType.byCode(((Number) typeCodeObj).intValue())
                != StateMetaInfoSnapshot.BackendStateType.OPERATOR) {
            return null;
        }
        Object serializerObj = metaInfo.get("serializer");
        if (serializerObj == null) {
            return null;
        }
        Map<String, String> serializer = JsonHelper.fromJson(serializerObj.toString(), HashMap.class);
        if (serializer == null || serializer.size() != 1) {
            return null;
        }
        String stateSerializerJson = serializer.get(OmniSerializerKey.STATE_SERIALIZER.getKey());
        TypeSerializer<?> stateSerializer = getFastOperatorStateSerializer(stateSerializerJson);
        if (stateSerializer == null) {
            return null;
        }

        String name = (String) metaInfo.get("name");
        Map<String, String> options = copyStringMap((Map<?, ?>) metaInfo.get("options"));
        String optionKey = StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE.toString();
        String stateTypeValue = options.get(optionKey);
        OmniSerializerOperatorStateMode stateType = OmniSerializerOperatorStateMode.get(stateTypeValue);
        if (stateType != null) {
            options.put(optionKey, stateType.getModeName());
        }

        Map<String, TypeSerializer<?>> serializerGroup = new HashMap<>();
        Map<String, TypeSerializerSnapshot<?>> serializerSnapshotGroup = new HashMap<>();
        String metaKey = StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString();
        serializerGroup.put(metaKey, stateSerializer);
        serializerSnapshotGroup.put(metaKey, stateSerializer.snapshotConfiguration());
        return new StateMetaInfoSnapshot(
                name,
                StateMetaInfoSnapshot.BackendStateType.OPERATOR,
                options,
                serializerSnapshotGroup,
                serializerGroup);
    }

    private static Map<String, String> copyStringMap(Map<?, ?> source) {
        Map<String, String> result = new HashMap<>();
        if (source == null) {
            return result;
        }
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                result.put(entry.getKey().toString(), entry.getValue().toString());
            }
        }
        return result;
    }

    private static TypeSerializer<?> getFastOperatorStateSerializer(String serializerJson) {
        if (serializerJson == null) {
            return null;
        }
        Map<String, Object> serializerMap = JsonHelper.fromJson(serializerJson, new TypeReference<Map<String, Object>>() {
        });
        Object typeObj = serializerMap == null ? null : serializerMap.get("type");
        if (!(typeObj instanceof Number)) {
            return null;
        }
        OmniSerializerType serializerType = OmniSerializerType.get(((Number) typeObj).intValue());
        if (serializerType == OmniSerializerType.LONG) {
            return LongSerializer.INSTANCE;
        }
        if (serializerType == OmniSerializerType.BYTE_PRIMITIVE_ARRAY) {
            return BytePrimitiveArraySerializer.INSTANCE;
        }
        return null;
    }

    public void writeOperatorMetaData(CheckpointStreamWithResultProvider provider,
                                      String operatorStateMetaInfoSnapshotsJson,
                                      String broadcastStateMetaInfoSnapshotsJson) throws IOException {
        try {
            byte[] metadata = getOrBuildOperatorMetadata(
                    operatorStateMetaInfoSnapshotsJson,
                    broadcastStateMetaInfoSnapshotsJson);

            omniTask.writeOperatorMetaDataBytes(provider, metadata);
        } catch (Exception e) {
            LOG.error("method : writeOperatorMetaData -> exception", e);
            throw new IOException("Failed to materialize operator metadata", e);
        }
    }

    public long getSavepointOutputStreamPos(CheckpointStreamWithResultProvider provider) throws Exception {
        return omniTask.getSavepointOutputStreamPos(provider);
    }

    public List<HandleAndLocalPath> uploadFilesToCheckpointFs(String pathsJson,
                                                              int numberOfSnapshottingThreads) throws IOException {
        final List<String> pathStrs = OBJECT_MAPPER.readValue(pathsJson, new TypeReference<List<String>>() {});
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

    public boolean callDownloadFileToLocal(StreamStateHandle restoreFileHandle, Path restoreTargetPath)
        throws IOException {
        FSDataInputStream inputStream = null;
        OutputStream outputStream = null;

        try {
            inputStream = restoreFileHandle.openInputStream();
            if (inputStream == null) {
                LOG.error("Error: callDownloadFileToLocal: inputStream is null");
                return false;
            }
            Files.createDirectories(restoreTargetPath.getParent());
            outputStream = Files.newOutputStream(restoreTargetPath);
            if (outputStream == null) {
                LOG.error("Error: callDownloadFileToLocal: outputStream is null");
                return false;
            }

            byte[] buffer = new byte[8 * 1024];
            while (true) {
                int numBytes = inputStream.read(buffer);
                if (numBytes == -1) {
                    break;
                }

                outputStream.write(buffer, 0, numBytes);
            }
            return true;
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }

            if (outputStream != null) {
                outputStream.close();
            }
        }
    }

    private IncrementalLocalKeyedStateHandle deserializeIncrementalLocalKeyedStateHandle(String metaStateHandleStr) {
        try {
            JsonNode rootNode = OBJECT_MAPPER.readTree(metaStateHandleStr);
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

    private IncrementalRemoteKeyedStateHandle deserializeIncrementalRemoteKeyedStateHandle(String metaStateHandleStr) {
        try {
            JsonNode rootNode = OBJECT_MAPPER.readTree(metaStateHandleStr);
            List<HandleAndLocalPath> sharedState = new ArrayList<>();
            JsonNode sharedStateNode = rootNode.get("sharedState").get(1);
            for (JsonNode stateNode : sharedStateNode) {
                String localPath = stateNode.get("localPath").asText();
                StreamStateHandle handle = TaskStateSnapshotDeser.parseStreamStateHandle(stateNode.get("handle"));
                sharedState.add(HandleAndLocalPath.of(handle, localPath));
            }

            List<HandleAndLocalPath> privateState = new ArrayList<>();
            JsonNode privateStateNode = rootNode.get("privateState").get(1);
            for (JsonNode stateNode : privateStateNode) {
                String localPath = stateNode.get("localPath").asText();
                StreamStateHandle handle = TaskStateSnapshotDeser.parseStreamStateHandle(stateNode.get("handle"));
                privateState.add(HandleAndLocalPath.of(handle, localPath));
            }

            UUID backendIdentifier = UUID.fromString(rootNode.get("backendIdentifier").asText());
            long checkpointId = rootNode.get("checkpointId").asLong();
            KeyGroupRange keyGroupRange = JsonHelper.fromJson(
                    rootNode.get("keyGroupRange").toString(),
                    KeyGroupRange.class);
            long persistedSizeOfThisCheckpoint = rootNode.get("persistedSizeOfThisCheckpoint").asLong();
            StreamStateHandle metaDataState =
                    TaskStateSnapshotDeser.parseStreamStateHandle(rootNode.get("metaDataState"));
            String jstateHandleId = rootNode.get("stateHandleId").get("keyString").asText();
            StateHandleID stateHandleId = new StateHandleID(jstateHandleId);

            return IncrementalRemoteKeyedStateHandle.restore(
                    backendIdentifier,
                    keyGroupRange,
                    checkpointId,
                    sharedState,
                    privateState,
                    metaDataState,
                    persistedSizeOfThisCheckpoint,
                    stateHandleId);
        } catch (JsonHelper.JsonHelperException ex) {
            throw ex;
        } catch (Exception e) {
            throw new JsonHelper.JsonHelperException(
                    "Error deserializing metaStateHandleStr to IncrementalRemoteKeyedStateHandle: "
                            + metaStateHandleStr, e);
        }
    }

    private KeyGroupsStateHandle deserializeKeyGroupsStateHandle(String metaStateHandleStr) {
        try {
            JsonNode rootNode = OBJECT_MAPPER.readTree(metaStateHandleStr);
            KeyGroupRange keyGroupRange = JsonHelper.fromJson(
                    rootNode.get("keyGroupRange").toString(),
                    KeyGroupRange.class);

            KeyGroupRangeOffsets keyGroupRangeOffsets = parseKeyGroupRangeOffsets(rootNode, keyGroupRange);

            JsonNode delegateNode = getFirstPresent(rootNode, "metaDataState", "streamStateHandle", "stateHandle");
            if (delegateNode == null) {
                throw new IOException("KeyGroupsStateHandle missing metaDataState/streamStateHandle/stateHandle.");
            }
            StreamStateHandle metaDataState =
                    TaskStateSnapshotDeser.parseStreamStateHandle(delegateNode);

            JsonNode stateHandleIdNode = rootNode.get("stateHandleId");
            String jstateHandleId = stateHandleIdNode != null && stateHandleIdNode.has("keyString")
                    ? stateHandleIdNode.get("keyString").asText()
                    : UUID.randomUUID().toString();
            StateHandleID stateHandleId = new StateHandleID(jstateHandleId);

            if (isKeyGroupsSavepointStateHandle(rootNode)) {
                return new KeyGroupsSavepointStateHandle(keyGroupRangeOffsets, metaDataState);
            }
            return KeyGroupsStateHandle.restore(
                    keyGroupRangeOffsets,
                    metaDataState,
                    stateHandleId);
        } catch (Exception e) {
            LOG.error("Error deserializing metaStateHandleStr to KeyGroupsStateHandle: metaStateHandleStr={}, exception={}",
                metaStateHandleStr, e.getMessage());
            throw new JsonHelper.JsonHelperException(
                    "Error deserializing metaStateHandleStr to IncrementalRemoteKeyedStateHandle: "
                    + metaStateHandleStr, e);
        }
    }

    private static KeyGroupRangeOffsets parseKeyGroupRangeOffsets(JsonNode rootNode, KeyGroupRange keyGroupRange) {
        JsonNode groupRangeOffsetsNode = rootNode.get("groupRangeOffsets");
        JsonNode offsetsNode = groupRangeOffsetsNode == null ? null : unwrapTypedArray(groupRangeOffsetsNode.get("offsets"));
        long[] offsets = new long[keyGroupRange.getNumberOfKeyGroups()];
        if (offsetsNode != null && offsetsNode.isArray()) {
            for (int i = 0; i < offsets.length && i < offsetsNode.size(); i++) {
                offsets[i] = offsetsNode.get(i).asLong();
            }
        }
        return new KeyGroupRangeOffsets(keyGroupRange, offsets);
    }

    private static boolean isKeyGroupsSavepointStateHandle(JsonNode rootNode) {
        String classType = rootNode.has("@class") ? rootNode.get("@class").asText() : "";
        String stateHandleName = rootNode.has("stateHandleName") ? rootNode.get("stateHandleName").asText() : "";
        return classType.contains("KeyGroupsSavepointStateHandle")
                || stateHandleName.contains("KeyGroupsSavepointStateHandle");
    }

    private static JsonNode getFirstPresent(JsonNode rootNode, String... fieldNames) {
        if (rootNode == null || rootNode.isNull()) {
            return null;
        }
        for (String fieldName : fieldNames) {
            JsonNode node = rootNode.get(fieldName);
            if (node != null && !node.isNull()) {
                return node;
            }
        }
        return null;
    }

    private static JsonNode unwrapTypedArray(JsonNode node) {
        if (node != null && node.isArray() && node.size() == 2 && node.get(0).isTextual()
                && node.get(1).isArray()) {
            return node.get(1);
        }
        return node;
    }

    private static boolean isStateHandleType(String classType, String simpleName, String fullClassName) {
        return simpleName.equals(classType) || fullClassName.equals(classType);
    }

    private OperatorStreamStateHandle deserializeOperatorStreamStateHandle(String metaStateHandleStr) {
        try {
            JsonNode rootNode = OBJECT_MAPPER.readTree(metaStateHandleStr);

            JsonNode delegateNode = getFirstPresent(
                    rootNode, "metaDataState", "delegateStateHandle", "streamStateHandle");
            if (delegateNode == null) {
                LOG.error("Error: OperatorStreamStateHandle missing metaDataState/delegateStateHandle/streamStateHandle. "
                        + "metaStateHandleStr={}", metaStateHandleStr);
                throw new IOException(
                        "OperatorStreamStateHandle missing metaDataState/delegateStateHandle/streamStateHandle.");
            }
            StreamStateHandle metaDataState = TaskStateSnapshotDeser.parseStreamStateHandle(delegateNode);
            if (metaDataState == null) {
                LOG.error("Error: OperatorStreamStateHandle delegate parsed to null. metaStateHandleStr={}",
                        metaStateHandleStr);
                throw new IOException("OperatorStreamStateHandle delegate parsed to null.");
            }
            JsonNode partitionOffsetsNode = rootNode.get("stateNameToPartitionOffsets");

            Map<String, StateMetaInfo> stateMap = new HashMap<>();

            if (partitionOffsetsNode != null && partitionOffsetsNode.isObject()) {
                Iterator<String> fieldNames = partitionOffsetsNode.fieldNames();
                while (fieldNames.hasNext()) {
                    String stateName = fieldNames.next();
                    if ("@class".equals(stateName)) {
                        continue;
                    }
                    JsonNode stateNode = partitionOffsetsNode.get(stateName);
                    if (stateNode == null || !stateNode.isObject()) {
                        continue;
                    }
                    JsonNode offsetsNode = unwrapTypedArray(stateNode.get("offsets"));
                    JsonNode distributionModeNode = stateNode.get("distributionMode");
                    if (offsetsNode != null && offsetsNode.isArray() && distributionModeNode != null
                            && distributionModeNode.isTextual()) {
                        int size = offsetsNode.size();
                        long[] offsets = new long[size];
                        for (int i = 0; i < size; i++) {
                            JsonNode offsetNode = offsetsNode.get(i);
                            if (offsetNode.isNumber()) {
                                offsets[i] = offsetNode.asLong();
                            }
                        }
                        StateMetaInfo metaInfo = new StateMetaInfo(
                                offsets, Mode.valueOf(distributionModeNode.asText()));
                        stateMap.put(stateName, metaInfo);
                    }
                }
            }
            return new OperatorStreamStateHandle(stateMap, metaDataState);
        } catch (Exception e) {
            LOG.error("Error: deserializing metaStateHandleStr to OperatorStreamStateHandle. metaStateHandleStr={}",
                    metaStateHandleStr, e);
            throw new JsonHelper.JsonHelperException(
                    "Error deserializing metaStateHandleStr to OperatorStreamStateHandle: "
                            + metaStateHandleStr, e);
        }
    }

    // This function is for C++ calling readMetaData in RocksDBIncrementalRestoreOperation
    public <K> String readMetaData(String metaStateHandleStr) throws IOException {
        // Reconstruct a IncrementalLocalStateHandle
        StreamStateHandle metaStateHandle = null;
        JsonNode rootNode = OBJECT_MAPPER.readTree(metaStateHandleStr);
        String classType = rootNode.get("@class").asText();
        if ("org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle".equals(classType)) {
            IncrementalLocalKeyedStateHandle localKeyedStateHandle =
                    deserializeIncrementalLocalKeyedStateHandle(metaStateHandleStr);
            metaStateHandle = localKeyedStateHandle.getMetaDataState();
        } else if ("org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle".equals(classType)) {
            IncrementalRemoteKeyedStateHandle remoteKeyedStateHandle =
                    deserializeIncrementalRemoteKeyedStateHandle(metaStateHandleStr);
            metaStateHandle = remoteKeyedStateHandle.getMetaStateHandle();
        } else if ("org.apache.flink.runtime.state.KeyGroupsStateHandle".equals(classType)
                || "org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle".equals(classType)
                || "KeyGroupsStateHandle".equals(classType)
                || "KeyGroupsSavepointStateHandle".equals(classType)) {
            KeyGroupsStateHandle keyedGroupsStateHandle =
                    deserializeKeyGroupsStateHandle(metaStateHandleStr);
            metaStateHandle = keyedGroupsStateHandle.getDelegateStateHandle();
        } else {
            throw new IOException("Unsupported metaStateHandleStr json.");
        }

        RuntimeEnvironment env = omniTask.getCheckpointingEnv();
        ClassLoader userCodeClassLoader = env.getUserCodeClassLoader().asClassLoader();
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
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();

            List<Map<String, Object>> stateMetaInfoSnapshotList = new ArrayList<>(stateMetaInfoSnapshots.size());
            for (StateMetaInfoSnapshot metaInfo : stateMetaInfoSnapshots) {
                stateMetaInfoSnapshotList.add(OmniStateSerializerHelper.buildSerializerJsonInfo(metaInfo));
            }

            LOG.debug("method : readMetaData -> stateMetaInfoSnapshotList : {}", JsonHelper.toJson(stateMetaInfoSnapshotList));

            // Convert to a string and return to C++
            return JsonHelper.toJson(stateMetaInfoSnapshotList);
        } finally {
            if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
                inputStream.close();
            }
        }
    }

    public <K> String readOperatorMetaData(String metaStateHandleStr) throws IOException {
        // Reconstruct a IncrementalLocalStateHandle
        StreamStateHandle metaStateHandle = null;
        JsonNode rootNode = OBJECT_MAPPER.readTree(metaStateHandleStr);
        String classType = rootNode.get("@class").asText();
        if ("org.apache.flink.runtime.state.OperatorStreamStateHandle".equals(classType)) {
            OperatorStreamStateHandle operatorStateHandle = deserializeOperatorStreamStateHandle(metaStateHandleStr);
            metaStateHandle = operatorStateHandle.getDelegateStateHandle();
        } else {
            LOG.error("Error: unsupported operator metaStateHandleStr json. classType={}, metaStateHandleStr={}",
                    classType, metaStateHandleStr);
            throw new IOException("Unsupported metaStateHandleStr json.");
        }
        if (metaStateHandle == null) {
            LOG.error("Error: OperatorStreamStateHandle delegate is null. metaStateHandleStr={}", metaStateHandleStr);
            throw new IOException("OperatorStreamStateHandle delegate is null.");
        }

        RuntimeEnvironment env = omniTask.getCheckpointingEnv();
        ClassLoader userCodeClassLoader = env.getUserCodeClassLoader().asClassLoader();
        InputStream inputStream = null;
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        ClassLoader restoreClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            // The readMetaData function
            inputStream = metaStateHandle.openInputStream();
            cancelStreamRegistry.registerCloseable(inputStream);

            Thread.currentThread().setContextClassLoader(userCodeClassLoader);
            OperatorBackendSerializationProxy backendSerializationProxy =
                new OperatorBackendSerializationProxy(userCodeClassLoader);
            backendSerializationProxy.read(new DataInputViewStreamWrapper(inputStream));

            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                backendSerializationProxy.getOperatorStateMetaInfoSnapshots();

            List<StateMetaInfoSnapshot> broadcastStateMetaInfoSnapshots =
                backendSerializationProxy.getBroadcastStateMetaInfoSnapshots();

            List<Map<String, Object>> stateMetaInfoSnapshotList = new ArrayList<>(stateMetaInfoSnapshots.size());
            for (StateMetaInfoSnapshot metaInfo : stateMetaInfoSnapshots) {
                stateMetaInfoSnapshotList.add(OmniStateSerializerHelper.buildSerializerJsonInfo(metaInfo));
            }

            LOG.debug("method : readOperatorMetaData -> stateMetaInfoSnapshotList : {}", JsonHelper.toJson(stateMetaInfoSnapshotList));

            // Convert to a string and return to C++
            return JsonHelper.toJson(stateMetaInfoSnapshotList);
        } finally {
            Thread.currentThread().setContextClassLoader(restoreClassLoader);
            if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
                inputStream.close();
            }
        }
    }

    public FSDataInputStream getSavepointInputStream(String metaStateHandleStr) throws IOException {
        StreamStateHandle metaStateHandle;
        KeyedStateHandleFormat keyedStateHandleFormat = null;
        JsonNode rootNode = OBJECT_MAPPER.readTree(metaStateHandleStr);
        JsonNode classTypeNode = getFirstPresent(rootNode, "@class", "stateHandleName");
        if (classTypeNode == null || !classTypeNode.isTextual()) {
            LOG.error("Error: Savepoint input stream handle json is missing @class/stateHandleName. "
                    + "metaStateHandleStr={}", metaStateHandleStr);
            throw new IOException("Savepoint input stream handle json is missing @class/stateHandleName.");
        }
        String classType = classTypeNode.asText();
        if (isStateHandleType(
                classType, "KeyGroupsStateHandle", "org.apache.flink.runtime.state.KeyGroupsStateHandle")
                || isStateHandleType(
                        classType,
                        "KeyGroupsSavepointStateHandle",
                        "org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle")) {
            KeyGroupsStateHandle keyedGroupsStateHandle = deserializeKeyGroupsStateHandle(metaStateHandleStr);
            keyedStateHandleFormat = keyedGroupsStateHandle instanceof KeyGroupsSavepointStateHandle
                    ? KeyedStateHandleFormat.CANONICAL_FULL_SNAPSHOT
                    : KeyedStateHandleFormat.NATIVE_HEAP;
            metaStateHandle = keyedGroupsStateHandle.getDelegateStateHandle();
        } else if (isStateHandleType(
                classType, "OperatorStreamStateHandle", "org.apache.flink.runtime.state.OperatorStreamStateHandle")) {
            OperatorStreamStateHandle operatorStateHandle = deserializeOperatorStreamStateHandle(metaStateHandleStr);
            metaStateHandle = operatorStateHandle.getDelegateStateHandle();
        } else {
            LOG.error("Error: unsupported savepoint input stream handle json. classType={}, metaStateHandleStr={}",
                    classType, metaStateHandleStr);
            throw new IOException("Unsupported savepoint input stream handle json: " + classType);
        }
        if (null == metaStateHandle) {
            LOG.error("Error getSavepointInputStream: metaStateHandleStr:{}", metaStateHandleStr);
            return null;
        }
        FSDataInputStream inputStream = metaStateHandle.openInputStream();
        if (null == inputStream) {
            LOG.error("Error getSavepointInputStream: metaStateHandleStr:{}", metaStateHandleStr);
        } else if (keyedStateHandleFormat != null) {
            keyedStateInputContexts.put(inputStream, new KeyedStateInputContext(keyedStateHandleFormat));
        }
        return inputStream;
    }

    public void closeSavepointInputStream(FSDataInputStream inputStream) throws IOException {
        if (inputStream != null) {
            KeyedStateInputContext context = keyedStateInputContexts.remove(inputStream);
            if (context != null) {
                context.closeNativeCursor();
            }
            inputStream.close();
        }
    }

    public void setSavepointInputStreamOffset(FSDataInputStream inputStream, long offset) throws IOException {
        if (inputStream != null) {
            KeyedStateInputContext context = keyedStateInputContexts.get(inputStream);
            if (context != null) {
                context.closeNativeCursor();
            }
            inputStream.seek(offset);
        }
    }

    public int readSavepointInputStream(FSDataInputStream inputStream, byte[] buffer, int offset, int length)
            throws IOException {
        if (inputStream == null) {
            return -1;
        }
        if (buffer == null) {
            LOG.error("Error: readSavepointInputStream target buffer is null.");
            throw new IOException("readSavepointInputStream target buffer is null.");
        }
        return inputStream.read(buffer, offset, length);
    }

    public boolean isUsingKeyGroupCompression(FSDataInputStream inputStream) throws IOException {
        DataInputView in = new DataInputViewStreamWrapper(inputStream);
        KeyedBackendSerializationProxy<?> serializationProxy =
                new KeyedBackendSerializationProxy<>(getUserCodeClassLoader());
        serializationProxy.read(in);
        KeyedStateInputContext context = keyedStateInputContexts.get(inputStream);
        if (context != null) {
            context.readVersion = serializationProxy.getReadVersion();
            context.usingKeyGroupCompression = serializationProxy.isUsingKeyGroupCompression();
            context.stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();
            TypeSerializerSnapshot<?> keySerializerSnapshot = serializationProxy.getKeySerializerSnapshot();
            context.keySerializer = keySerializerSnapshot == null ? null : keySerializerSnapshot.restoreSerializer();
            context.keyGroupPrefixBytes = computeRequiredBytesInKeyGroupPrefix(getMaxNumberOfKeyGroups());
            context.nativeStateReaderInfos = createNativeStateReaderInfos(context.stateMetaInfoSnapshots);
        }
        return serializationProxy.isUsingKeyGroupCompression();
    }

    public KeyGroupEntryWrapper getKeyGroupEntries(FSDataInputStream inputStream, int currentKvStateId,
                                        boolean isUsingKeyGroupCompression) throws IOException {
        KeyedStateInputContext context = keyedStateInputContexts.get(inputStream);
        if (context != null && context.format == KeyedStateHandleFormat.NATIVE_HEAP) {
            return getNativeHeapKeyGroupEntries(inputStream, currentKvStateId, context);
        }
        StreamCompressionDecorator keygroupStressCompressionDecorator = isUsingKeyGroupCompression ?
            SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;
        try(InputStream compressedKgIn = keygroupStressCompressionDecorator.decorateWithCompression(inputStream);
            DataInputViewStreamWrapper kgInputView = new DataInputViewStreamWrapper(compressedKgIn)) {
            // first time
            if (currentKvStateId == -1) {
                currentKvStateId = END_OF_KEY_GROUP_MARK & kgInputView.readShort();
            }
            int entryStateId = currentKvStateId;
            KeyGroupEntry[] keyGroupEntries = new KeyGroupEntry[MAX_KEY_GROUP_ENTRY_BATCH_SIZE];
            // read by state or by count 1000
            int count = 0;
            for (int i = 0; i < MAX_KEY_GROUP_ENTRY_BATCH_SIZE; i++) {
                count++;
                byte[] key = BytePrimitiveArraySerializer.INSTANCE.deserialize(kgInputView);
                byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(kgInputView);
                // 通过 key[0] & FIRST_BIT_IN_BYTE_MASK 可以判断是否应该读取下一个kvStateId的数据；
                if (hasMetaDataFollowsFlag(key)) {
                    // 清除key[0] 的标识信息
                    clearMetaDataFollowsFlag(key);
                    currentKvStateId = END_OF_KEY_GROUP_MARK & kgInputView.readShort();
                    keyGroupEntries[i] = new KeyGroupEntry(key, value);
                    break;
                }
                keyGroupEntries[i] = new KeyGroupEntry(key, value);
            }
            return new KeyGroupEntryWrapper(keyGroupEntries, currentKvStateId, entryStateId, count);
        }
    }

    private KeyGroupEntryWrapper getNativeHeapKeyGroupEntries(
            FSDataInputStream inputStream,
            int currentKvStateId,
            KeyedStateInputContext context) throws IOException {
        if (!context.isNativeReaderReady()) {
            throw new IOException("Native heap keyed state reader is not initialized from metadata.");
        }
        if (currentKvStateId == -1 || context.nativeCursor == null || context.nativeCursor.finished) {
            context.closeNativeCursor();
            context.nativeCursor = new NativeHeapKeyGroupCursor(
                    inputStream,
                    context.usingKeyGroupCompression,
                    context.readVersion,
                    context.nativeStateReaderInfos);
        }
        try {
            KeyGroupEntryWrapper entries = context.nativeCursor.nextBatch(
                    context.keySerializer,
                    context.keyGroupPrefixBytes);
            if (context.nativeCursor.finished) {
                context.closeNativeCursor();
            }
            return entries;
        } catch (IOException | RuntimeException e) {
            context.closeNativeCursor();
            throw e;
        }
    }

    private ClassLoader getUserCodeClassLoader() {
        RuntimeEnvironment env = omniTask.getCheckpointingEnv();
        return env.getUserCodeClassLoader().asClassLoader();
    }

    private int getMaxNumberOfKeyGroups() {
        return omniTask.getTaskInfo().getMaxNumberOfParallelSubtasks();
    }

    private static int computeRequiredBytesInKeyGroupPrefix(int totalKeyGroupsInJob) {
        return totalKeyGroupsInJob > SINGLE_BYTE_MAX_GROUPS ? PREFIX_TWO_BYTES : PREFIX_ONE_BYTE;
    }

    private static List<NativeStateReaderInfo> createNativeStateReaderInfos(
            List<StateMetaInfoSnapshot> metaInfoSnapshots) throws IOException {
        List<NativeStateReaderInfo> infos = new ArrayList<>(metaInfoSnapshots.size());
        for (StateMetaInfoSnapshot metaInfoSnapshot : metaInfoSnapshots) {
            TypeSerializer<?> namespaceSerializer = null;
            TypeSerializer<?> valueSerializer = null;
            if (metaInfoSnapshot.getBackendStateType() == StateMetaInfoSnapshot.BackendStateType.KEY_VALUE) {
                namespaceSerializer = restoreSerializer(
                        metaInfoSnapshot,
                        StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER);
                valueSerializer = restoreSerializer(
                        metaInfoSnapshot,
                        StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
            } else if (metaInfoSnapshot.getBackendStateType()
                    == StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE) {
                valueSerializer = restoreSerializer(
                        metaInfoSnapshot,
                        StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
            }
            infos.add(new NativeStateReaderInfo(
                    metaInfoSnapshot.getName(),
                    metaInfoSnapshot.getBackendStateType(),
                    namespaceSerializer,
                    valueSerializer));
        }
        return infos;
    }

    private static TypeSerializer<?> restoreSerializer(
            StateMetaInfoSnapshot metaInfoSnapshot,
            StateMetaInfoSnapshot.CommonSerializerKeys serializerKey) throws IOException {
        TypeSerializer<?> serializer = metaInfoSnapshot.getTypeSerializer(serializerKey.toString());
        if (serializer != null) {
            return serializer;
        }
        TypeSerializerSnapshot<?> snapshot = metaInfoSnapshot.getTypeSerializerSnapshot(serializerKey);
        if (snapshot == null) {
            throw new IOException("Missing serializer snapshot " + serializerKey
                    + " for state " + metaInfoSnapshot.getName());
        }
        return snapshot.restoreSerializer();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void serializeWithSerializer(
            TypeSerializer serializer,
            Object value,
            DataOutputSerializer output) throws IOException {
        serializer.serialize(value, output);
    }

    private static void writeKeyGroupPrefix(
            DataOutputSerializer output,
            int keyGroup,
            int keyGroupPrefixBytes) throws IOException {
        for (int i = keyGroupPrefixBytes; --i >= 0;) {
            output.writeByte((keyGroup >> (i * 8)) & 0xFF);
        }
    }

    private static byte[] copyBuffer(DataOutputSerializer output) {
        return output.getCopyOfBuffer();
    }

    private enum KeyedStateHandleFormat {
        CANONICAL_FULL_SNAPSHOT,
        NATIVE_HEAP
    }

    private static class KeyedStateInputContext {
        private final KeyedStateHandleFormat format;
        private int readVersion;
        private boolean usingKeyGroupCompression;
        private List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = Collections.emptyList();
        private TypeSerializer<?> keySerializer;
        private int keyGroupPrefixBytes;
        private List<NativeStateReaderInfo> nativeStateReaderInfos = Collections.emptyList();
        private NativeHeapKeyGroupCursor nativeCursor;

        private KeyedStateInputContext(KeyedStateHandleFormat format) {
            this.format = format;
        }

        private boolean isNativeReaderReady() {
            return keySerializer != null
                    && keyGroupPrefixBytes > 0
                    && stateMetaInfoSnapshots != null
                    && nativeStateReaderInfos != null
                    && nativeStateReaderInfos.size() == stateMetaInfoSnapshots.size();
        }

        private void closeNativeCursor() throws IOException {
            if (nativeCursor != null) {
                nativeCursor.close();
                nativeCursor = null;
            }
        }
    }

    private static class NativeStateReaderInfo {
        private final String stateName;
        private final StateMetaInfoSnapshot.BackendStateType backendStateType;
        private final TypeSerializer<?> namespaceSerializer;
        private final TypeSerializer<?> valueSerializer;

        private NativeStateReaderInfo(
                String stateName,
                StateMetaInfoSnapshot.BackendStateType backendStateType,
                TypeSerializer<?> namespaceSerializer,
                TypeSerializer<?> valueSerializer) {
            this.stateName = stateName;
            this.backendStateType = backendStateType;
            this.namespaceSerializer = namespaceSerializer;
            this.valueSerializer = valueSerializer;
        }
    }

    private static class NativeHeapKeyGroupCursor implements AutoCloseable {
        private final DataInputViewStreamWrapper inputView;
        private final int keyGroupId;
        private final int readVersion;
        private final List<NativeStateReaderInfo> stateReaderInfos;
        private int nextStateOrdinal;
        private int currentKvStateId = -1;
        private List<KeyGroupEntry> pendingEntries = Collections.emptyList();
        private int pendingIndex;
        private boolean finished;

        private NativeHeapKeyGroupCursor(
                FSDataInputStream inputStream,
                boolean usingKeyGroupCompression,
                int readVersion,
                List<NativeStateReaderInfo> stateReaderInfos) throws IOException {
            this.keyGroupId = new DataInputViewStreamWrapper(inputStream).readInt();
            StreamCompressionDecorator compressionDecorator = usingKeyGroupCompression
                    ? SnappyStreamCompressionDecorator.INSTANCE
                    : UncompressedStreamCompressionDecorator.INSTANCE;
            InputStream compressedInputStream = null;
            boolean inputViewCreated = false;
            try {
                compressedInputStream = compressionDecorator.decorateWithCompression(
                        new NonClosingInputStream(inputStream));
                this.inputView = new DataInputViewStreamWrapper(compressedInputStream);
                inputViewCreated = true;
            } finally {
                if (!inputViewCreated && compressedInputStream != null) {
                    compressedInputStream.close();
                }
            }
            this.readVersion = readVersion;
            this.stateReaderInfos = stateReaderInfos;
        }

        private KeyGroupEntryWrapper nextBatch(
                TypeSerializer<?> keySerializer,
                int keyGroupPrefixBytes) throws IOException {
            while (pendingIndex >= pendingEntries.size() && !finished) {
                readNextState(keySerializer, keyGroupPrefixBytes);
            }
            if (pendingIndex >= pendingEntries.size()) {
                finished = true;
                return new KeyGroupEntryWrapper(
                        new KeyGroupEntry[0],
                        END_OF_KEY_GROUP_MARK,
                        END_OF_KEY_GROUP_MARK,
                        0);
            }

            int batchKvStateId = currentKvStateId;
            int remaining = pendingEntries.size() - pendingIndex;
            int count = Math.min(MAX_KEY_GROUP_ENTRY_BATCH_SIZE, remaining);
            KeyGroupEntry[] batch = new KeyGroupEntry[count];
            for (int i = 0; i < count; i++) {
                batch[i] = pendingEntries.get(pendingIndex++);
            }
            if (pendingIndex >= pendingEntries.size() && nextStateOrdinal >= stateReaderInfos.size()) {
                finished = true;
            }
            int nextKvStateId = finished ? END_OF_KEY_GROUP_MARK : currentKvStateId;
            return new KeyGroupEntryWrapper(batch, nextKvStateId, batchKvStateId, count);
        }

        private void readNextState(
                TypeSerializer<?> keySerializer,
                int keyGroupPrefixBytes) throws IOException {
            if (nextStateOrdinal >= stateReaderInfos.size()) {
                finished = true;
                pendingEntries = Collections.emptyList();
                pendingIndex = 0;
                return;
            }
            currentKvStateId = inputView.readShort() & END_OF_KEY_GROUP_MARK;
            if (currentKvStateId < 0 || currentKvStateId >= stateReaderInfos.size()) {
                throw new IOException("Invalid native heap kvStateId " + currentKvStateId
                        + " for keyGroup " + keyGroupId
                        + ", stateCount=" + stateReaderInfos.size());
            }
            NativeStateReaderInfo readerInfo = stateReaderInfos.get(currentKvStateId);
            if (readerInfo.backendStateType == StateMetaInfoSnapshot.BackendStateType.KEY_VALUE) {
                pendingEntries = readKeyValueStateEntries(keySerializer, keyGroupPrefixBytes, readerInfo);
            } else if (readerInfo.backendStateType == StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE) {
                pendingEntries = readPriorityQueueEntries(keyGroupPrefixBytes, readerInfo);
            } else {
                throw new IOException("Unsupported native heap backend state type: "
                        + readerInfo.backendStateType + ", state=" + readerInfo.stateName);
            }
            pendingIndex = 0;
            nextStateOrdinal++;
        }

        private List<KeyGroupEntry> readKeyValueStateEntries(
                TypeSerializer<?> keySerializer,
                int keyGroupPrefixBytes,
                NativeStateReaderInfo readerInfo) throws IOException {
            if (readVersion == 1) {
                return readKeyValueStateEntriesV1(keySerializer, keyGroupPrefixBytes, readerInfo);
            }
            int numElements = inputView.readInt();
            List<KeyGroupEntry> result = new ArrayList<>(numElements);
            for (int i = 0; i < numElements; i++) {
                Object namespace = readerInfo.namespaceSerializer.deserialize(inputView);
                Object key = keySerializer.deserialize(inputView);
                Object state = readerInfo.valueSerializer.deserialize(inputView);
                result.add(toKeyValueEntry(
                        keySerializer,
                        readerInfo.namespaceSerializer,
                        readerInfo.valueSerializer,
                        keyGroupPrefixBytes,
                        keyGroupId,
                        key,
                        namespace,
                        state));
            }
            return result;
        }

        private List<KeyGroupEntry> readKeyValueStateEntriesV1(
                TypeSerializer<?> keySerializer,
                int keyGroupPrefixBytes,
                NativeStateReaderInfo readerInfo) throws IOException {
            if (inputView.readByte() == 0) {
                return Collections.emptyList();
            }
            int numNamespaces = inputView.readInt();
            List<KeyGroupEntry> result = new ArrayList<>();
            for (int namespaceIndex = 0; namespaceIndex < numNamespaces; namespaceIndex++) {
                Object namespace = readerInfo.namespaceSerializer.deserialize(inputView);
                int numEntries = inputView.readInt();
                for (int entryIndex = 0; entryIndex < numEntries; entryIndex++) {
                    Object key = keySerializer.deserialize(inputView);
                    Object state = readerInfo.valueSerializer.deserialize(inputView);
                    result.add(toKeyValueEntry(
                            keySerializer,
                            readerInfo.namespaceSerializer,
                            readerInfo.valueSerializer,
                            keyGroupPrefixBytes,
                            keyGroupId,
                            key,
                            namespace,
                            state));
                }
            }
            return result;
        }

        private List<KeyGroupEntry> readPriorityQueueEntries(
                int keyGroupPrefixBytes,
                NativeStateReaderInfo readerInfo) throws IOException {
            int numElements = inputView.readInt();
            List<KeyGroupEntry> result = new ArrayList<>(numElements);
            for (int i = 0; i < numElements; i++) {
                Object element = readerInfo.valueSerializer.deserialize(inputView);
                DataOutputSerializer keyOutput = new DataOutputSerializer(128);
                writeKeyGroupPrefix(keyOutput, keyGroupId, keyGroupPrefixBytes);
                serializeWithSerializer(readerInfo.valueSerializer, element, keyOutput);
                result.add(new KeyGroupEntry(copyBuffer(keyOutput), new byte[0]));
            }
            return result;
        }

        private KeyGroupEntry toKeyValueEntry(
                TypeSerializer<?> keySerializer,
                TypeSerializer<?> namespaceSerializer,
                TypeSerializer<?> valueSerializer,
                int keyGroupPrefixBytes,
                int keyGroup,
                Object key,
                Object namespace,
                Object state) throws IOException {
            DataOutputSerializer keyOutput = new DataOutputSerializer(128);
            writeKeyGroupPrefix(keyOutput, keyGroup, keyGroupPrefixBytes);
            serializeWithSerializer(keySerializer, key, keyOutput);
            serializeWithSerializer(namespaceSerializer, namespace, keyOutput);

            DataOutputSerializer valueOutput = new DataOutputSerializer(128);
            serializeWithSerializer(valueSerializer, state, valueOutput);
            return new KeyGroupEntry(copyBuffer(keyOutput), copyBuffer(valueOutput));
        }

        @Override
        public void close() throws IOException {
            inputView.close();
        }
    }

    private static final class NonClosingInputStream extends FilterInputStream {
        private NonClosingInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() throws IOException {
            // The compressed stream is closed with the native cursor, while the underlying
            // FSDataInputStream is reused across key groups and closed by closeSavepointInputStream().
        }
    }

    private static class OperatorMetadataCacheKey {
        private final JobID jobId;
        private final String operatorStateMetaInfoSnapshotsJson;
        private final String broadcastStateMetaInfoSnapshotsJson;

        private OperatorMetadataCacheKey(
                JobID jobId,
                String operatorStateMetaInfoSnapshotsJson,
                String broadcastStateMetaInfoSnapshotsJson) {
            this.jobId = jobId;
            this.operatorStateMetaInfoSnapshotsJson = operatorStateMetaInfoSnapshotsJson;
            this.broadcastStateMetaInfoSnapshotsJson = broadcastStateMetaInfoSnapshotsJson;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof OperatorMetadataCacheKey)) {
                return false;
            }
            OperatorMetadataCacheKey that = (OperatorMetadataCacheKey) other;
            return Objects.equals(jobId, that.jobId)
                    && Objects.equals(operatorStateMetaInfoSnapshotsJson, that.operatorStateMetaInfoSnapshotsJson)
                    && Objects.equals(broadcastStateMetaInfoSnapshotsJson, that.broadcastStateMetaInfoSnapshotsJson);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, operatorStateMetaInfoSnapshotsJson, broadcastStateMetaInfoSnapshotsJson);
        }
    }

}
