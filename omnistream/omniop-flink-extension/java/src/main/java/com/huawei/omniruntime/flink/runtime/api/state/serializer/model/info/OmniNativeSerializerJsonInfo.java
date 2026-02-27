package com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info;

import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.SC;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

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
    private List<String> fieldNames;
    private List<OmniNativeSerializerJsonInfo> fieldSerializers;

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
                + "fieldNames = [" + String.join(SC.BLANK + SC.COMMA, fieldNameInfoList) + "], "
                + "fieldSerializers = [" + String.join(SC.BLANK + SC.COMMA, fieldSerializerInfoList) + "]"
                + "}";
    }
}
