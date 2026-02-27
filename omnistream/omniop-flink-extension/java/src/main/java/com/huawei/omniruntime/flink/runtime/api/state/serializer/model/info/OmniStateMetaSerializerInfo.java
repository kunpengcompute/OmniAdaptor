package com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info;

import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.SC;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OmniStateMetaSerializerInfo
 *
 */

public class OmniStateMetaSerializerInfo implements Serializable {
    private static final long serialVersionUID = -9140371400302434862L;

    private String stateName;
    private StateMetaInfoSnapshot.BackendStateType backendStateType;
    private final Map<String, String> options = new HashMap<>();
    private final Map<String, TypeSerializer<?>> serializerGroup = new HashMap<>();
    private final Map<String, TypeSerializerSnapshot<?>> serializerSnapshotGroup = new HashMap<>();

    /**
     * disabled new instantiation
     */
    private OmniStateMetaSerializerInfo() {
    }

    /**
     * disabled new instantiation
     */
    private OmniStateMetaSerializerInfo(Builder builder) {
        this.stateName = builder.stateName;
        this.backendStateType = builder.backendStateType;
        this.options.putAll(builder.options);
        this.serializerGroup.putAll(builder.serializerGroup);
        this.serializerSnapshotGroup.putAll(builder.serializerSnapshotGroup);
    }

    public String getStateName() {
        return this.stateName;
    }

    public Map<String, TypeSerializer<?>> getSerializerGroup() {
        return this.serializerGroup;
    }

    public Map<String, TypeSerializerSnapshot<?>> getSerializerSnapshotGroup() {
        return this.serializerSnapshotGroup;
    }

    public TypeSerializer<?> getKeySerializer() {
        return this.serializerGroup.get(StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER.toString());
    }

    public TypeSerializer<?> getNamespaceSerializer() {
        return this.serializerGroup.get(StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString());
    }

    public TypeSerializer<?> getValueSerializer() {
        return this.serializerGroup.get(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString());
    }

    public TypeSerializerSnapshot<?> getKeySerializerSnapshot() {
        return this.serializerSnapshotGroup.get(StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER.toString());
    }

    public TypeSerializerSnapshot<?> getNamespaceSerializerSnapshot() {
        return this.serializerSnapshotGroup.get(StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString());
    }

    public TypeSerializerSnapshot<?> getValueSerializerSnapshot() {
        return this.serializerSnapshotGroup.get(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString());
    }

    public synchronized static Builder builder() {
        return new Builder();
    }

    public OmniStateMetaSerializerInfo copy() {
        return new Builder()
                .backendStateType(this.backendStateType)
                .stateName(this.stateName)
                .options(this.options)
                .serializerGroup(this.serializerGroup)
                .build();
    }

    @Override
    public String toString() {
        List<String> serializerList = new ArrayList<>();
        for (Map.Entry<String, TypeSerializer<?>> serializer : this.serializerGroup.entrySet()) {
            if (null == serializer) {
                continue;
            }
            serializerList.add("key = " + serializer.getKey() + ", " + serializer.getValue());
        }
        List<String> serializerSnapshotList = new ArrayList<>();
        for (Map.Entry<String, TypeSerializerSnapshot<?>> serializer : this.serializerSnapshotGroup.entrySet()) {
            if (null == serializer) {
                continue;
            }
            serializerSnapshotList.add("key = " + serializer.getKey() + ", " + serializer.getValue());
        }
        return "OmniStateMetaSerializerInfo {"
                + "stateName = " + this.stateName + ", "
                + "backendStateType = " + this.backendStateType + ", "
                + "options = " + this.options + ", "
                + "serializerGroup = {" + String.join(SC.BLANK + SC.COMMA, serializerList) + "}, "
                + "serializerSnapshotGroup = {" + String.join(SC.BLANK + SC.COMMA, serializerSnapshotList) + "}"
                + "}";
    }

    /**
     * Builder
     *
     */
    public static class Builder implements Serializable {
        private static final long serialVersionUID = 3008370937836010927L;

        private String stateName;
        private StateMetaInfoSnapshot.BackendStateType backendStateType;
        private final Map<String, String> options = new HashMap<>();
        private final Map<String, TypeSerializer<?>> serializerGroup = new HashMap<>();
        private final Map<String, TypeSerializerSnapshot<?>> serializerSnapshotGroup = new HashMap<>();

        /**
         * disabled new instantiation
         */
        private Builder() {
        }

        public Builder stateName(String stateName) {
            this.stateName = stateName;
            return this;
        }

        public Builder backendStateType(StateMetaInfoSnapshot.BackendStateType backendStateType) {
            this.backendStateType = backendStateType;
            return this;
        }

        public Builder options(Map<String, String> options) {
            if (null != options) {
                this.options.putAll(options);
            }
            return this;
        }

        public Builder serializerGroup(Map<String, TypeSerializer<?>> serializerGroup) {
            if (null != serializerGroup) {
                for (Map.Entry<String, TypeSerializer<?>> serializer : serializerGroup.entrySet()) {
                    if (StringUtils.isEmpty(serializer.getKey()) || null == serializer.getValue()) {
                        continue;
                    }
                    this.serializerGroup.put(serializer.getKey(), serializer.getValue());
                    this.serializerSnapshotGroup.put(serializer.getKey(), serializer.getValue().snapshotConfiguration());
                }
            }
            return this;
        }

        public Builder serializerGroup(String key, TypeSerializer<?> value) {
            if (StringUtils.isNotEmpty(key) && null != value) {
                this.serializerGroup.put(key, value);
                this.serializerSnapshotGroup.put(key, value.snapshotConfiguration());
            }
            return this;
        }

        public boolean serializerGroupContainsKey(String key) {
            if (StringUtils.isNotEmpty(key)) {
                return this.serializerGroup.containsKey(key);
            }
            return false;
        }

        /**
         * build
         *
         * @return OmniStateMetaSerializerInfo
         */
        public OmniStateMetaSerializerInfo build() {
            return new OmniStateMetaSerializerInfo(this);
        }
    }
}
