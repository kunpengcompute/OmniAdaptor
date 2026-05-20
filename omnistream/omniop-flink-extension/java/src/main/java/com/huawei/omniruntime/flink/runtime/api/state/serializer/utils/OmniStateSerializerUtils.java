package com.huawei.omniruntime.flink.runtime.api.state.serializer.utils;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.OmniStateSerializerHelper;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.SC;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerOperatorStateMode;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniStateMetaSerializerInfo;
import com.huawei.omniruntime.flink.runtime.taskmanager.OmniTask;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProviderImpl;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

public class OmniStateSerializerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(OmniStateSerializerUtils.class);

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
        if(stateMetaInfoMapList.isEmpty()) {
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
