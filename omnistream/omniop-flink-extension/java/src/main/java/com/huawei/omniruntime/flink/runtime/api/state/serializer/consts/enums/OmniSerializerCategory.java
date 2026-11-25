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
 * OmniSerializerCategory
 *
 */

public enum OmniSerializerCategory {
    BASIC,
    DATE,
    LIST,
    MAP,
    TUPLE,
    POJO,
    VOID_NAMESPACE,
    TIMER,
    PRIMITIVE_ARRAY,
    ROW,
    BINARY_ROW,
    EXTERNAL,
    TIME_WINDOW,
    UNKNOWN
}
