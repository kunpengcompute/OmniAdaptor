package com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums;

import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

/**
 * OmniSerializerJson
 *
 */

public enum OmniSerializerKey {
    KEY_SERIALIZER("keySerializer", StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER),
    NAMESPACE_SERIALIZER("namespaceSerializer", StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER),
    VALUE_SERIALIZER("valueSerializer", StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER),
    /* native 返回，目的是与 MapSerializer 做区分，此处兼容处理 */
    STATE_SERIALIZER("stateSerializer", StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER),
    ;

    private final String key;
    private final StateMetaInfoSnapshot.CommonSerializerKeys metaKey;

    OmniSerializerKey(String key, StateMetaInfoSnapshot.CommonSerializerKeys metaKey) {
        this.key = key;
        this.metaKey = metaKey;
    }

    public String getKey() {
        return this.key;
    }

    public StateMetaInfoSnapshot.CommonSerializerKeys getMetaKey() {
        return this.metaKey;
    }

    public String getMetaKeyStr() {
        return this.metaKey.toString();
    }

    public boolean equals(String key) {
        return this.key.equalsIgnoreCase(key);
    }

    public boolean equals(StateMetaInfoSnapshot.CommonSerializerKeys metaKey) {
        return this.metaKey.equals(metaKey);
    }

    public boolean equalsMetaKey(String metaKey) {
        return this.getMetaKeyStr().equalsIgnoreCase(metaKey);
    }

    public static OmniSerializerKey get(String key) {
        if (null == key) {
            return null;
        }

        for (OmniSerializerKey item : OmniSerializerKey.values()) {
            if (item.equals(key)) {
                return item;
            }
        }

        return null;
    }

    public static OmniSerializerKey getBy(String metaKey) {
        if (null == metaKey) {
            return null;
        }

        for (OmniSerializerKey item : OmniSerializerKey.values()) {
            if (item.equalsMetaKey(metaKey)) {
                return item;
            }
        }

        return null;
    }
}
