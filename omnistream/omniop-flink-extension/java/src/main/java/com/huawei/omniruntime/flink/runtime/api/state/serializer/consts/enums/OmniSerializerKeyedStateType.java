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

import org.apache.flink.api.common.state.StateDescriptor;

/**
 * OmniSerializerKeyedStateType
 *
 */

public enum OmniSerializerKeyedStateType {
    UNKNOWN("0", "UNKNOWN", StateDescriptor.Type.UNKNOWN),
    VALUE("1", "VALUE", StateDescriptor.Type.VALUE),
    LIST("2", "LIST", StateDescriptor.Type.LIST),
    REDUCING("3", "REDUCING", StateDescriptor.Type.REDUCING),
    FOLDING("4", "FOLDING", StateDescriptor.Type.FOLDING),
    AGGREGATING("5", "AGGREGATING", StateDescriptor.Type.AGGREGATING),
    MAP("6", "MAP", StateDescriptor.Type.MAP),
    ;

    private final String code;
    private final String name;
    private final StateDescriptor.Type type;

    OmniSerializerKeyedStateType(String code, String name, StateDescriptor.Type type) {
        this.code = code;
        this.name = name;
        this.type = type;
    }

    public StateDescriptor.Type getType() {
        return this.type;
    }

    public String getName() {
        return this.name;
    }

    public String getTypeName() {
        return this.type.name();
    }

    public boolean equals(String str) {
        return this.code.equalsIgnoreCase(str) || this.name.equalsIgnoreCase(str);
    }

    public static OmniSerializerKeyedStateType get(String str) {
        if(null == str){
            return null;
        }
        for (OmniSerializerKeyedStateType item : OmniSerializerKeyedStateType.values()) {
            if (item.equals(str)) {
                return item;
            }
        }

        return null;
    }
}
