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
    NAMESPACE_SERIALIZER("namespaceSerializer"),
    FIELD_NAMES("fieldNames"),
    FIELD_SERIALIZERS("fieldSerializers"),
    LOGICAL_TYPE("logicalType"),
    FIELDS("fields"),
    FIELD_NAME("fieldName"),
    FIELD_SERIALIZER("fieldSerializer"),
    SERIALIZER_ATTRIBUTES("serializerAttributes"),
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
