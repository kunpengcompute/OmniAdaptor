package com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums;

import org.apache.flink.api.common.state.StateDescriptor;

/**
 * OmniSerializerKeyedStateType
 *
 */

public enum OmniSerializerKeyedStateType {
    UNKNOW("0", StateDescriptor.Type.UNKNOWN),
    VALUE("1", StateDescriptor.Type.VALUE),
    LIST("2", StateDescriptor.Type.LIST),
    REDUCING("3", StateDescriptor.Type.REDUCING),
    FOLDING("4", StateDescriptor.Type.FOLDING),
    AGGREGATING("5", StateDescriptor.Type.AGGREGATING),
    MAP("6", StateDescriptor.Type.MAP),
    ;

    private final String code;
    private final StateDescriptor.Type type;

    OmniSerializerKeyedStateType(String code, StateDescriptor.Type type) {
        this.code = code;
        this.type = type;
    }

    public StateDescriptor.Type getType() {
        return this.type;
    }

    public String getTypeName() {
        return this.type.name();
    }

    public boolean equals(String code) {
        return this.code.equalsIgnoreCase(code);
    }

    public static OmniSerializerKeyedStateType get(String code) {
        for (OmniSerializerKeyedStateType item : OmniSerializerKeyedStateType.values()) {
            if (item.equals(code)) {
                return item;
            }
        }

        return null;
    }
}