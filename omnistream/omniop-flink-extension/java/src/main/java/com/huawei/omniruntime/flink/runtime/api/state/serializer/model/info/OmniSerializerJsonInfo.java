package com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info;

import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.SC;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * OmniSerializerJsonInfo
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OmniSerializerJsonInfo implements Serializable {
    private static final long serialVersionUID = 6753209368135180619L;

    private String serializerName;
    private String clazz;
    private OmniSerializerJsonInfo elementSerializer;
    private OmniSerializerJsonInfo keySerializer;
    private OmniSerializerJsonInfo valueSerializer;
    private List<String> fields;
    private List<OmniSerializerJsonInfo> fieldSerializers;

    public String getSerializerName() {
        return serializerName;
    }

    public void setSerializerName(String serializerName) {
        this.serializerName = serializerName;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    public OmniSerializerJsonInfo getElementSerializer() {
        return elementSerializer;
    }

    public void setElementSerializer(OmniSerializerJsonInfo elementSerializer) {
        this.elementSerializer = elementSerializer;
    }

    public OmniSerializerJsonInfo getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(OmniSerializerJsonInfo keySerializer) {
        this.keySerializer = keySerializer;
    }

    public OmniSerializerJsonInfo getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(OmniSerializerJsonInfo valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public List<OmniSerializerJsonInfo> getFieldSerializers() {
        return fieldSerializers;
    }

    public void setFieldSerializers(List<OmniSerializerJsonInfo> fieldSerializers) {
        this.fieldSerializers = fieldSerializers;
    }

    @Override
    public String toString() {
        List<String> fieldInfoList = new ArrayList<>();
        if (null != this.fields) {
            fieldInfoList.addAll(this.fields);
        }
        List<String> fieldSerializerInfoList = new ArrayList<>();
        if (null != this.fieldSerializers) {
            for (OmniSerializerJsonInfo fieldSerializer : this.fieldSerializers) {
                fieldSerializerInfoList.add(fieldSerializer.toString());
            }
        }
        return "OmniSerializerJsonInfo {"
                + "serializerName = " + this.serializerName + ", "
                + "clazz = " + this.clazz + ", "
                + "elementSerializer = " + (null == this.elementSerializer ? null : this.elementSerializer.toString()) + ", "
                + "keySerializer = " + (null == this.keySerializer ? null : this.keySerializer.toString()) + ", "
                + "valueSerializer = " + (null == this.valueSerializer ? null : this.valueSerializer.toString()) + ", "
                + "fields = [" + String.join(SC.BLANK + SC.COMMA, fieldInfoList) + "], "
                + "fieldSerializers = [" + String.join(SC.BLANK + SC.COMMA, fieldSerializerInfoList) + "]"
                + "}";
    }
}
