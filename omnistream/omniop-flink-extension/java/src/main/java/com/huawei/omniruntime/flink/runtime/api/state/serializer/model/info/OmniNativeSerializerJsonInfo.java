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

import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.SC;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * OmniNativeSerializerJsonInfo
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OmniNativeSerializerJsonInfo implements Serializable {
    private static final long serialVersionUID = -2254373665089927519L;

    private int type;
    private OmniSerializerType serializerType;
    private String elementType;
    private Class<?> elementTypeClazz;
    private OmniNativeSerializerJsonInfo keySerializer;
    private OmniNativeSerializerJsonInfo valueSerializer;
    private OmniNativeSerializerJsonInfo namespaceSerializer;
    private List<String> fieldNames;
    private List<OmniNativeSerializerJsonInfo> fieldSerializers;
    private JsonNode logicalType;
    private OmniSerializerAttributesInfo serializerAttributes;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public OmniSerializerType getSerializerType() {
        return serializerType;
    }

    public void setSerializerType(OmniSerializerType serializerType) {
        this.serializerType = serializerType;
    }

    public String getElementType() {
        return elementType;
    }

    public void setElementType(String elementType) {
        this.elementType = elementType;
    }

    public Class<?> getElementTypeClazz() {
        return elementTypeClazz;
    }

    public void setElementTypeClazz(Class<?> elementTypeClazz) {
        this.elementTypeClazz = elementTypeClazz;
    }

    public OmniNativeSerializerJsonInfo getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(OmniNativeSerializerJsonInfo keySerializer) {
        this.keySerializer = keySerializer;
    }

    public OmniNativeSerializerJsonInfo getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(OmniNativeSerializerJsonInfo valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public OmniNativeSerializerJsonInfo getNamespaceSerializer() {
        return namespaceSerializer;
    }

    public void setNamespaceSerializer(OmniNativeSerializerJsonInfo namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public List<OmniNativeSerializerJsonInfo> getFieldSerializers() {
        return fieldSerializers;
    }

    public void setFieldSerializers(List<OmniNativeSerializerJsonInfo> fieldSerializers) {
        this.fieldSerializers = fieldSerializers;
    }

    public JsonNode getLogicalType() {
        return logicalType;
    }

    public void setLogicalType(JsonNode logicalType) {
        this.logicalType = logicalType;
    }

    public OmniSerializerAttributesInfo getSerializerAttributes() {
        return serializerAttributes;
    }

    public void setSerializerAttributes(OmniSerializerAttributesInfo serializerAttributes) {
        this.serializerAttributes = serializerAttributes;
    }

    @Override
    public String toString() {
        List<String> fieldNameInfoList = new ArrayList<>();
        if (null != this.fieldNames) {
            fieldNameInfoList.addAll(this.fieldNames);
        }
        List<String> fieldSerializerInfoList = new ArrayList<>();
        if (null != this.fieldSerializers) {
            for (OmniNativeSerializerJsonInfo fieldSerializer : this.fieldSerializers) {
                fieldSerializerInfoList.add(fieldSerializer.toString());
            }
        }
        return "OmniNativeSerializerJsonInfo {"
                + "type = " + this.type + ", "
                + "serializerType = " + this.serializerType + ", "
                + "elementType = " + this.elementType + ", "
                + "elementTypeClazz = " + this.elementTypeClazz + ", "
                + "keySerializer = " + (null == this.keySerializer ? null : this.keySerializer.toString()) + ", "
                + "valueSerializer = " + (null == this.valueSerializer ? null : this.valueSerializer.toString()) + ", "
                + "namespaceSerializer = " + (null == this.namespaceSerializer ? null : this.namespaceSerializer.toString()) + ", "
                + "fieldNames = [" + String.join(SC.BLANK + SC.COMMA, fieldNameInfoList) + "], "
                + "fieldSerializers = [" + String.join(SC.BLANK + SC.COMMA, fieldSerializerInfoList) + "]"
                + "logicalType = " + (null == this.logicalType ? null : this.logicalType.toString()) + ", "
                + "serializerAttributes = " + (null == this.serializerAttributes ? null : this.serializerAttributes.toString()) + ", "
                + "}";
    }
}
