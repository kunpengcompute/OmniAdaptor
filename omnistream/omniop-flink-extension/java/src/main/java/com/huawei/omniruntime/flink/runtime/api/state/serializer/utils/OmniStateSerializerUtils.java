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

package com.huawei.omniruntime.flink.runtime.api.state.serializer.utils;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.OmniStateSerializerHelper;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.SC;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerOperatorStateMode;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerType;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniStateMetaSerializerInfo;
import com.huawei.omniruntime.flink.runtime.metrics.exception.GeneralRuntimeException;
import com.huawei.omniruntime.flink.runtime.taskmanager.OmniTask;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProviderImpl;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonDeserializer;
import org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class OmniStateSerializerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(OmniStateSerializerUtils.class);

    private static final LogicalTypeJsonSerializer LOGICAL_TYPE_JSON_SERIALIZER = new LogicalTypeJsonSerializer();
    private static final LogicalTypeJsonDeserializer LOGICAL_TYPE_JSON_DESERIALIZER = new LogicalTypeJsonDeserializer();

    public static Class<?> getSerializerInstance(TypeSerializer<?> typeSerializer, OmniSerializerType serializerType) {
        Object serializerInstance = typeSerializer.createInstance();
        if (null != serializerInstance) {
            return serializerInstance.getClass();
        } else if (null != serializerType.getClazz()) {
            return serializerType.getClazz();
        }

        return null;
    }

    public static String getSerializerInstanceClazz(TypeSerializer<?> typeSerializer, OmniSerializerType serializerType) {
        Class<?> clazz = getSerializerInstance(typeSerializer, serializerType);
        return null == clazz ? null : clazz.getName();
    }

    public static String firstNonBlank(String... values) {
        return Arrays.stream(values)
                .filter(Objects::nonNull)
                .filter(StringUtils::isNotEmpty)
                .findFirst()
                .orElse(null);
    }

    public static Class<?> classForName(String name, boolean initialize, ClassLoader loader) {
        if (StringUtils.isEmpty(name)) {
            return null;
        }
        try {
            String className = name.replace(SC.UNDERSCORE, SC.DOT);
            LOG.error("method : classForName -> className : {}", className);
            return Class.forName(className, initialize, loader);
        } catch (ClassNotFoundException e) {
            LOG.error("method : classForName -> format exception", e);
            throw new GeneralRuntimeException(String.format("Could not find class '%s' for unsafe operations.", name), e);
        }
    }

    public static <T> T objectFormat(Object obj, Class<T> clazz) {
        try {
            if (null == obj) {
                return null;
            }
            return JsonHelper.fromJson(JsonHelper.toJson(obj), clazz);
        } catch (Exception e) {
            LOG.error("method : objectFormat -> format exception", e);
            throw new GeneralRuntimeException(e);
        }
    }

    public static JsonNode logicalTypeJsonSerialize(LogicalType logicalType) {
        try {
            StringWriter stringWriter = new StringWriter();
            try (JsonGenerator jsonGenerator = JsonHelper.getObjectMapper().createGenerator(stringWriter)) {
                LOGICAL_TYPE_JSON_SERIALIZER.serialize(logicalType, jsonGenerator, null);
                jsonGenerator.flush();
            }
            String jsonString = stringWriter.toString();
            return JsonHelper.fromJson(jsonString, JsonNode.class);
        } catch (Exception e) {
            LOG.error("method : logicalTypeJsonDeserialize -> serialize exception", e);
            throw new GeneralRuntimeException(e);
        }
    }

    public static LogicalType logicalTypeJsonDeserialize(JsonParser logicalTypeJsonParser, DeserializationContext ctx) {
        try {
            return LOGICAL_TYPE_JSON_DESERIALIZER.deserialize(logicalTypeJsonParser, ctx);
        } catch (Exception e) {
            LOG.error("method : logicalTypeJsonDeserialize -> deserialize exception", e);
            throw new GeneralRuntimeException(e);
        }
    }

    public static LogicalType logicalTypeJsonDeserialize(JsonNode logicalTypeJsonNode) {
        try (JsonParser jsonParser = JsonHelper.getObjectMapper().treeAsTokens(logicalTypeJsonNode)) {
            return logicalTypeJsonDeserialize(jsonParser, null);
        } catch (Exception e) {
            LOG.error("method : logicalTypeJsonDeserialize -> deserialize exception", e);
            throw new GeneralRuntimeException(e);
        }
    }

    public static RowType getRowType(LogicalType[] types) {
        AtomicInteger i = new AtomicInteger();
        List<RowType.RowField> fields = Arrays.stream(types)
                .map(type -> new RowType.RowField("f" + i.getAndIncrement(), type, SC.EMPTY))
                .collect(Collectors.toList());
        return new RowType(fields);
    }

    public static LocalRecoveryConfig parseLocalRecoveryConfig(String localRecoveryConfigStr) {
        LocalRecoveryConfig recoveryConfig = null;
        if (!"{}".equals(localRecoveryConfigStr)) {
            Map<String, Object> configMap = JsonHelper.fromJson(localRecoveryConfigStr, new TypeReference<Map<String, Object>>() {
            });
            List<String> dirs = (List<String>) configMap.get(ConfigKey.ALLOCATION_BASE_DIRS.getCode());
            File[] files = new File[dirs.size()];
            for (int i = 0; i < dirs.size(); i++) {
                files[i] = new File(dirs.get(i));
            }

            String jobIdHexStr = (String) configMap.get(ConfigKey.JOB_ID.getCode());
            String jobVertexIdHexStr = (String) configMap.get(ConfigKey.JOB_VERTEX_ID.getCode());

            JobID jobID = JobID.fromHexString(jobIdHexStr);
            JobVertexID jobVertexID = JobVertexID.fromHexString(jobVertexIdHexStr);

            int subtaskIndex = (Integer) configMap.get(ConfigKey.SUBTASK_INDEX.getCode());
            LocalRecoveryDirectoryProvider provider = new LocalRecoveryDirectoryProviderImpl(files, jobID, jobVertexID, subtaskIndex);
            recoveryConfig = new LocalRecoveryConfig(provider);
        }

        return recoveryConfig;
    }

    public static List<StateMetaInfoSnapshot> buildStateMetaInfoSnapshot(OmniTask omniTask, List<Map<String, Object>> stateMetaInfoMapList) {
        List<StateMetaInfoSnapshot> resultList = new ArrayList<>(stateMetaInfoMapList.size());
        if (stateMetaInfoMapList.isEmpty()) {
            return resultList;
        }

        ExecutionConfig executionConfig = omniTask.getExecutionConfig();
        ClassLoader userCodeClassLoader = omniTask.getCheckpointingEnv()
                .getUserCodeClassLoader().asClassLoader();

        for (Map<String, Object> metaInfo : stateMetaInfoMapList) {
            String name = (String) metaInfo.get(MetaInfoKey.NAME.getCode());
            int typeCode = (Integer) metaInfo.get(MetaInfoKey.BACKEND_STATE_TYPE.getCode());

            Map<String, String> options = (Map<String, String>) metaInfo.get(MetaInfoKey.OPTIONS.getCode());
            String stateTypeValue = options.get(StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE.toString());
            OmniSerializerOperatorStateMode stateType = OmniSerializerOperatorStateMode.get(stateTypeValue);
            if (null == stateType) {
                LOG.warn("method : buildStateMetaInfoSnapshot -> keyedStateTypeValue : {} undefined.", stateTypeValue);
            } else {
                options.put(StateMetaInfoSnapshot.CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE.toString(), stateType.getModeName());
            }

            Map<String, String> serializer = JsonHelper.fromJson(metaInfo.get(MetaInfoKey.SERIALIZER.getCode()).toString(), HashMap.class);
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
            LOG.debug("method : buildStateMetaInfoSnapshot -> serializerInfo : {}", serializerInfo);

            resultList.add(new StateMetaInfoSnapshot(
                    name,
                    StateMetaInfoSnapshot.BackendStateType.byCode(typeCode),
                    options,
                    null == serializerInfo ? Collections.emptyMap() : serializerInfo.getSerializerSnapshotGroup(),
                    null == serializerInfo ? Collections.emptyMap() : serializerInfo.getSerializerGroup()));
        }

        return resultList;
    }

    enum MetaInfoKey {
        NAME("name"),
        BACKEND_STATE_TYPE("backendStateType"),
        OPTIONS("options"),
        SERIALIZER("serializer"),
        ;

        private final String code;

        MetaInfoKey(String code) {
            this.code = code;
        }

        public String getCode() {
            return this.code;
        }
    }

    enum ConfigKey {
        ALLOCATION_BASE_DIRS("allocationBaseDirs"),
        JOB_ID("jobID"),
        JOB_VERTEX_ID("jobVertexID"),
        SUBTASK_INDEX("subtaskIndex"),
        ;

        private final String code;

        ConfigKey(String code) {
            this.code = code;
        }

        public String getCode() {
            return this.code;
        }
    }
}
