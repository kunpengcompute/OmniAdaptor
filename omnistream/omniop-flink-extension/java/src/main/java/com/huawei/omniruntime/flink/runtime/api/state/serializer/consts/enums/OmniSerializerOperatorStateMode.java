package com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums;

import org.apache.flink.runtime.state.OperatorStateHandle;

public enum OmniSerializerOperatorStateMode {
    SPLIT_DISTRIBUTE("0", OperatorStateHandle.Mode.SPLIT_DISTRIBUTE),
    UNION("1", OperatorStateHandle.Mode.UNION),
    BROADCAST("2", OperatorStateHandle.Mode.BROADCAST),
    ;

    private final String code;
    private final OperatorStateHandle.Mode mode;

    OmniSerializerOperatorStateMode(String code, OperatorStateHandle.Mode mode) {
        this.code = code;
        this.mode = mode;
    }

    public OperatorStateHandle.Mode getMode() {
        return this.mode;
    }

    public String getModeName() {
        return this.mode.name();
    }

    public boolean equals(String code) {
        return this.code.equalsIgnoreCase(code);
    }

    public static OmniSerializerOperatorStateMode get(String code) {
        if(null == code){
            return null;
        }
        for (OmniSerializerOperatorStateMode item : OmniSerializerOperatorStateMode.values()) {
            if (item.equals(code)) {
                return item;
            }
        }

        return null;
    }
}
