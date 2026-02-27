package com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums;

/**
 * OmniSerializerJson
 *
 */

public enum OmniSerializerJson {
    TYPE("type"),
    ELEMENT_TYPE("element_type"),
    KEY_SERIALIZER("keySerializer"),
    VALUE_SERIALIZER("valueSerializer"),
    FIELD_NAMES("fieldNames"),
    FIELD_SERIALIZERS("fieldSerializers"),
    ;

    private final String key;

    OmniSerializerJson(String key) {
        this.key = key;
    }

    public String getKey() {
        return this.key;
    }

    public boolean equals(String key) {
        return this.key.equalsIgnoreCase(key);
    }

    public static OmniSerializerJson get(String key) {
        if (null == key) {
            return null;
        }

        for (OmniSerializerJson item : OmniSerializerJson.values()) {
            if (item.equals(key)) {
                return item;
            }
        }

        return null;
    }
}
