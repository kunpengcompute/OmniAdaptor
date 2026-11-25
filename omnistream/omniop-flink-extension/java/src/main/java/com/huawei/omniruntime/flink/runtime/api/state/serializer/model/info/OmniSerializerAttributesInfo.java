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

package com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

/**
 * OmniSerializerAttributesInfo
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OmniSerializerAttributesInfo implements Serializable {
    private static final long serialVersionUID = -1000437572704990162L;

    /**
     * @see org.apache.flink.api.common.typeutils.base.ListSerializer
     * @see org.apache.flink.api.java.typeutils.runtime.PojoSerializer
     * @see org.apache.flink.api.java.typeutils.runtime.TupleSerializer
     * @see org.apache.flink.table.runtime.typeutils.ExternalSerializer
     * @see org.apache.flink.table.types.DataType
     */
    private String clazzName;
    private Class<?> clazz;

    /**
     * @see org.apache.flink.table.runtime.typeutils.ExternalSerializer
     */
    private Boolean externalIsInternalInput;

    public String getClazzName() {
        return clazzName;
    }

    public void setClazzName(String clazzName) {
        this.clazzName = clazzName;
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public void setClazz(Class<?> clazz) {
        this.clazz = clazz;
    }

    public Boolean getExternalIsInternalInput() {
        return externalIsInternalInput;
    }

    public void setExternalIsInternalInput(Boolean externalIsInternalInput) {
        this.externalIsInternalInput = externalIsInternalInput;
    }

    public static OmniSerializerAttributesInfo ofClazz(String clazzName) {
        OmniSerializerAttributesInfo info = new OmniSerializerAttributesInfo();
        info.setClazzName(clazzName);
        return info;
    }

    public static OmniSerializerAttributesInfo ofList(String clazzName) {
        return ofClazz(clazzName);
    }

    public static OmniSerializerAttributesInfo ofPojo(String clazzName) {
        return ofClazz(clazzName);
    }

    public static OmniSerializerAttributesInfo ofTuple(String clazzName) {
        return ofClazz(clazzName);
    }

    public static OmniSerializerAttributesInfo ofExternal(Boolean IsInternalInput, String clazzName) {
        OmniSerializerAttributesInfo info = ofClazz(clazzName);
        info.setExternalIsInternalInput(IsInternalInput);
        return info;
    }

    @Override
    public String toString() {
        return "OmniSerializerAttributesInfo {"
                + "clazzName = " + this.clazzName + ", "
                + "externalIsInternalInput = " + this.externalIsInternalInput + ", "
                + "}";
    }
}
