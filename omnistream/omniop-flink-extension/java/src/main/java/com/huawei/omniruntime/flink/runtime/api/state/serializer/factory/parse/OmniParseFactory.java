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

package com.huawei.omniruntime.flink.runtime.api.state.serializer.factory.parse;

import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.SC;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerType;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniNativeSerializerJsonInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniSerializerAttributesInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniSerializerJsonInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.type.BinaryTypeInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.type.TimerTypeInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.type.WindowTypeInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.utils.OmniStateSerializerUtils;
import com.huawei.omniruntime.flink.runtime.metrics.exception.GeneralRuntimeException;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.VoidNamespaceTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * OmniParseFactory
 *
 */

public abstract class OmniParseFactory {
    private static final Logger LOG = LoggerFactory.getLogger(OmniParseFactory.class);

    public static final String TYPE_SERIALIZER_PRIVATE_KEY_CLAZZ = "clazz";
    public static final String TYPE_SERIALIZER_PRIVATE_KEY_FIELDS = "fields";
    public static final String TYPE_SERIALIZER_PRIVATE_KEY_TYPES = "types";
    public static final String TYPE_SERIALIZER_PRIVATE_KEY_FIELD_SERIALIZERS = "fieldSerializers";
    public static final String TYPE_SERIALIZER_PRIVATE_KEY_INTERNAL_SERIALIZERS = "internalSerializer";

    // recursion depth max
    protected static final int DEPTH_MAX = 100;
    // recursion depth start
    protected static final int DEPTH_START = 0;
    // recursion depth interval
    protected static final int DEPTH_INTERVAL = 1;

    public static OmniParseFactory build(OmniSerializerType serializerType) {
        if (null == serializerType) {
            return null;
        }
        OmniParseFactory factory = null;
        if (serializerType.isBasic()) {
            factory = new OmniParseValueFactory();
        } else if (serializerType.isPrimitiveArray()) {
            factory = new OmniParseValueFactory();
        } else {
            switch (serializerType) {
                case LIST:
                    factory = new OmniParseListFactory();
                    break;
                case MAP:
                    factory = new OmniParseMapFactory();
                    break;
                case POJO:
                case TUPLE:
                case VOID_NAMESPACE:
                case TIMER:
                case ROW:
                case BINARY_ROW:
                case EXTERNAL:
                case TIME_WINDOW:
                    factory = new OmniParseValueFactory();
                    break;
                case UNKNOWN:
                    break;
                default:
                    LOG.warn("method : build -> serializer type : {} has no deal.", serializerType);
                    break;
            }
        }

        return factory;
    }

    protected TypeInformation<?> buildTypeInformationBy(OmniNativeSerializerJsonInfo info, int depth) {
        if (depth > DEPTH_MAX) {
            throw new GeneralRuntimeException(String.format("max recursion depth (%s) exceeded. Input may be malformed or malicious.", DEPTH_MAX));
        }
        if (null == info || null == info.getSerializerType()) {
            return null;
        }
        if (info.getSerializerType().isBasic()) {
            return BasicTypeInfo.getInfoFor(info.getSerializerType().getClazz());
        } else if (info.getSerializerType().isPrimitiveArray()) {
            return PrimitiveArrayTypeInfo.getInfoFor(info.getSerializerType().getClazz());
        } else if (OmniSerializerType.LIST.equals(info.getSerializerType())) {
            OmniNativeSerializerJsonInfo valueSerializerInfo = info.getValueSerializer();
            TypeInformation<?> elementTypeInfo = (null == valueSerializerInfo)
                    ? TypeInformation.of(Object.class)
                    : buildTypeInformationBy(valueSerializerInfo, depth + DEPTH_INTERVAL);
            return Types.LIST(elementTypeInfo);
        } else if (OmniSerializerType.MAP.equals(info.getSerializerType())) {
            OmniNativeSerializerJsonInfo keySerializerInfo = info.getKeySerializer();
            OmniNativeSerializerJsonInfo valueSerializerInfo = info.getValueSerializer();
            TypeInformation<?> keyTypeInfo = (null == keySerializerInfo)
                    ? Types.STRING : buildTypeInformationBy(keySerializerInfo, depth + DEPTH_INTERVAL);
            TypeInformation<?> valueTypeInfo = (null == valueSerializerInfo)
                    ? TypeInformation.of(Object.class) : buildTypeInformationBy(valueSerializerInfo, depth + DEPTH_INTERVAL);
            return Types.MAP(keyTypeInfo, valueTypeInfo);
        } else if (OmniSerializerType.POJO.equals(info.getSerializerType())) {
            List<String> fieldNames = info.getFieldNames();
            List<OmniNativeSerializerJsonInfo> fieldInfos = info.getFieldSerializers();
            boolean hasFieldNames = fieldNames != null && !fieldNames.isEmpty();
            boolean hasFieldInfos = fieldInfos != null && !fieldInfos.isEmpty();
            if (hasFieldNames || hasFieldInfos) {
                if (!hasFieldNames || !hasFieldInfos || fieldNames.size() != fieldInfos.size()) {
                    throw new GeneralRuntimeException("POJO field names and serializers must have matching sizes");
                }
                Map<String, TypeInformation<?>> fieldTypes = new LinkedHashMap<>();
                for (int i = 0; i < fieldNames.size(); i++) {
                    String fieldName = fieldNames.get(i);
                    if (StringUtils.isBlank(fieldName) || fieldTypes.containsKey(fieldName)
                            || fieldInfos.get(i) == null) {
                        throw new GeneralRuntimeException("Invalid POJO field description at index " + i);
                    }
                    TypeInformation<?> fieldType = buildTypeInformationBy(fieldInfos.get(i), depth + DEPTH_INTERVAL);
                    if (fieldType == null) {
                        throw new GeneralRuntimeException("Unsupported POJO field type: " + fieldName);
                    }
                    fieldTypes.put(fieldName, fieldType);
                }
                return Types.POJO(info.getSerializerAttributes().getClazz(), fieldTypes);
            }
            return Types.POJO(info.getSerializerAttributes().getClazz());
        } else if (OmniSerializerType.TUPLE.equals(info.getSerializerType())) {
            // 优先用 C++ 端递归传过来的 fieldSerializers 重建带具体字段类型的 TupleTypeInfo；
            // 走 TypeExtractor.createTypeInfo(Tuple2.class) 会因为 raw type 拿到 GenericType，
            // 字段子序列化器与 C++ 端不匹配 → restore 出来的字节解析错位。
            List<OmniNativeSerializerJsonInfo> fieldInfos = info.getFieldSerializers();
            if (fieldInfos != null && !fieldInfos.isEmpty()) {
                TypeInformation<?>[] fieldTypes = new TypeInformation[fieldInfos.size()];
                for (int i = 0; i < fieldInfos.size(); i++) {
                    fieldTypes[i] = buildTypeInformationBy(fieldInfos.get(i), depth + DEPTH_INTERVAL);
                }
                Class<?> tupleClass = info.getSerializerAttributes().getClazz();
                if (tupleClass == null) {
                    // C++ 端漏传 element_type 时按 arity 兜底到 TupleN.class
                    tupleClass = Tuple.getTupleClass(fieldTypes.length);
                }
                return new TupleTypeInfo(tupleClass, fieldTypes);
            }
            // 兼容老 JSON：无 fieldSerializers 时退回基于 raw class 的解析
            return TypeExtractor.createTypeInfo(info.getSerializerAttributes().getClazz());
        } else if (OmniSerializerType.VOID_NAMESPACE.equals(info.getSerializerType())) {
            return new VoidNamespaceTypeInfo();
        } else if (OmniSerializerType.TIMER.equals(info.getSerializerType())) {
            OmniNativeSerializerJsonInfo keySerializerInfo = info.getKeySerializer();
            OmniNativeSerializerJsonInfo namespaceSerializerInfo = info.getNamespaceSerializer();
            TypeInformation<?> keyTypeInfo = (null == keySerializerInfo)
                    ? Types.STRING : buildTypeInformationBy(keySerializerInfo, depth + DEPTH_INTERVAL);
            TypeInformation<?> namespaceTypeInfo = (null == namespaceSerializerInfo)
                    ? new VoidNamespaceTypeInfo() : buildTypeInformationBy(namespaceSerializerInfo, depth + DEPTH_INTERVAL);
            return new TimerTypeInfo<>(keyTypeInfo, namespaceTypeInfo);
        } else if (OmniSerializerType.ROW.equals(info.getSerializerType())) {
            try {
                RowType rowType = (RowType) OmniStateSerializerUtils.logicalTypeJsonDeserialize(info.getLogicalType());
                return InternalTypeInfo.of(rowType);
            } catch (Exception e) {
                LOG.error("method : buildTypeInformationBy -> build row exception", e);
                throw new GeneralRuntimeException(e);
            }
        } else if (OmniSerializerType.BINARY_ROW.equals(info.getSerializerType())) {
            int arity = CollectionUtil.isNullOrEmpty(info.getFieldNames()) ? 0 : info.getFieldNames().size();
            return BinaryTypeInfo.of(new BinaryRowData(arity), info.getFieldNames());
        } else if (OmniSerializerType.EXTERNAL.equals(info.getSerializerType())) {
            try {
                OmniSerializerAttributesInfo serializerAttributes = info.getSerializerAttributes();
                boolean isInternalInput = null == serializerAttributes.getExternalIsInternalInput()
                        ? Boolean.FALSE : serializerAttributes.getExternalIsInternalInput();

                LogicalType logicalType = OmniStateSerializerUtils.logicalTypeJsonDeserialize(info.getLogicalType());
                DataType dataType = null == serializerAttributes.getClazz()
                        ? DataTypes.of(logicalType) : DataTypes.of(logicalType).bridgedTo(serializerAttributes.getClazz());

                return ExternalTypeInfo.of(dataType, isInternalInput);
            } catch (Exception e) {
                LOG.error("method : buildTypeInformationBy -> build external exception", e);
                throw new GeneralRuntimeException(e);
            }
        } else if (OmniSerializerType.TIME_WINDOW.equals(info.getSerializerType())) {
            return WindowTypeInfo.ofClass(info.getSerializerAttributes().getClazz());
        }

        return null;
    }

    protected OmniSerializerJsonInfo buildJsonInfoBy(TypeSerializer<?> typeSerializer, OmniSerializerType serializerType, int depth) {
        if (depth > DEPTH_MAX) {
            throw new GeneralRuntimeException(String.format("max recursion depth (%s) exceeded. Input may be malformed or malicious.", DEPTH_MAX));
        }
        if (null == typeSerializer || null == serializerType) {
            LOG.warn("method : buildJsonInfoBy -> typeSerializer or serializerType is null.");
            return null;
        }
        OmniSerializerJsonInfo jsonInfo = new OmniSerializerJsonInfo();
        jsonInfo.setSerializerName(typeSerializer.getClass().getName());
        Class<?> serializerInstance = OmniStateSerializerUtils.getSerializerInstance(typeSerializer, serializerType);
        String serializerInstanceName = null == serializerInstance ? null : serializerInstance.getName();
        jsonInfo.setSerializerInstanceClazz(serializerInstanceName);
        if (serializerType.isBasic()) {
            return jsonInfo;
        } else if (serializerType.isPrimitiveArray()) {
            return jsonInfo;
        } else if (OmniSerializerType.LIST.equals(serializerType)) {
            ListSerializer<?> listSerializer = (ListSerializer<?>) typeSerializer;
            OmniSerializerJsonInfo elementSerializerJsonInfo = (null == listSerializer.getElementSerializer())
                    ? null
                    : buildJsonInfoBy(
                    listSerializer.getElementSerializer(),
                    OmniSerializerType.get(listSerializer.getElementSerializer().getClass()),
                    depth + DEPTH_INTERVAL);
            jsonInfo.setElementSerializer(elementSerializerJsonInfo);
            jsonInfo.setSerializerAttributes(OmniSerializerAttributesInfo.ofList(serializerInstanceName));
            return jsonInfo;
        } else if (OmniSerializerType.MAP.equals(serializerType)) {
            MapSerializer<?, ?> mapSerializer = (MapSerializer<?, ?>) typeSerializer;
            OmniSerializerJsonInfo keySerializerJsonInfo = (null == mapSerializer.getKeySerializer())
                    ? null
                    : buildJsonInfoBy(
                    mapSerializer.getKeySerializer(),
                    OmniSerializerType.get(mapSerializer.getKeySerializer().getClass()),
                    depth + DEPTH_INTERVAL);
            OmniSerializerJsonInfo valueSerializerJsonInfo = (null == mapSerializer.getValueSerializer())
                    ? null
                    : buildJsonInfoBy(
                    mapSerializer.getValueSerializer(),
                    OmniSerializerType.get(mapSerializer.getValueSerializer().getClass()),
                    depth + DEPTH_INTERVAL);
            jsonInfo.setKeySerializer(keySerializerJsonInfo);
            jsonInfo.setValueSerializer(valueSerializerJsonInfo);
            return jsonInfo;
        } else if (OmniSerializerType.POJO.equals(serializerType)) {
            PojoSerializer<?> pojoSerializer = (PojoSerializer<?>) typeSerializer;
            Class<?> clazz = ReflectionUtils.retrievePrivateField(pojoSerializer, TYPE_SERIALIZER_PRIVATE_KEY_CLAZZ);
            Field[] fields = ReflectionUtils.retrievePrivateField(pojoSerializer, TYPE_SERIALIZER_PRIVATE_KEY_FIELDS);
            TypeSerializer<?>[] fieldSerializers = ReflectionUtils.retrievePrivateField(pojoSerializer, TYPE_SERIALIZER_PRIVATE_KEY_FIELD_SERIALIZERS);
            List<String> fieldInfoList = new ArrayList<>();
            if (null != fields) {
                for (Field field : fields) {
                    fieldInfoList.add(field.getName());
                }
            }
            List<OmniSerializerJsonInfo> fieldSerializerInfoList = new ArrayList<>();
            if (null != fieldSerializerInfoList) {
                for (TypeSerializer<?> fieldSerializer : fieldSerializers) {
                    OmniSerializerJsonInfo fieldSerializerJsonInfo = (null == fieldSerializer)
                            ? null
                            : buildJsonInfoBy(
                            fieldSerializer,
                            OmniSerializerType.get(fieldSerializer.getClass()),
                            depth + DEPTH_INTERVAL);
                    fieldSerializerInfoList.add(fieldSerializerJsonInfo);
                }
            }
            jsonInfo.setClazz(null == clazz ? SC.EMPTY : clazz.getName());
            jsonInfo.setFields(fieldInfoList);
            jsonInfo.setFieldSerializers(fieldSerializerInfoList);
            jsonInfo.setSerializerAttributes(OmniSerializerAttributesInfo.ofPojo(null == clazz ? SC.EMPTY : clazz.getName()));
            return jsonInfo;
        } else if (OmniSerializerType.TUPLE.equals(serializerType)) {
            TupleSerializer<?> tupleSerializer = (TupleSerializer<?>) typeSerializer;
            TypeSerializer<?>[] fieldSerializers = ReflectionUtils.retrievePrivateField(tupleSerializer, TYPE_SERIALIZER_PRIVATE_KEY_FIELD_SERIALIZERS);
            List<OmniSerializerJsonInfo> fieldSerializerInfoList = new ArrayList<>();
            if (null != fieldSerializerInfoList) {
                for (TypeSerializer<?> fieldSerializer : fieldSerializers) {
                    OmniSerializerJsonInfo fieldSerializerJsonInfo = (null == fieldSerializer)
                            ? null
                            : buildJsonInfoBy(
                            fieldSerializer,
                            OmniSerializerType.get(fieldSerializer.getClass()),
                            depth + DEPTH_INTERVAL);
                    fieldSerializerInfoList.add(fieldSerializerJsonInfo);
                }
            }
            jsonInfo.setFieldSerializers(fieldSerializerInfoList);
            jsonInfo.setSerializerAttributes(OmniSerializerAttributesInfo.ofTuple(tupleSerializer.getTupleClass().getName()));
            return jsonInfo;
        } else if (OmniSerializerType.VOID_NAMESPACE.equals(serializerType)) {
            return jsonInfo;
        } else if (OmniSerializerType.TIME_WINDOW.equals(serializerType)) {
            jsonInfo.setSerializerAttributes(OmniSerializerAttributesInfo.ofClazz(serializerInstanceName));
            return jsonInfo;
        } else if (OmniSerializerType.TIMER.equals(serializerType)) {
            TimerSerializer<?, ?> timerSerializer = (TimerSerializer<?, ?>) typeSerializer;
            OmniSerializerJsonInfo keySerializerJsonInfo = (null == timerSerializer.getKeySerializer())
                    ? null
                    : buildJsonInfoBy(
                    timerSerializer.getKeySerializer(),
                    OmniSerializerType.get(timerSerializer.getKeySerializer().getClass()),
                    depth + DEPTH_INTERVAL);
            OmniSerializerJsonInfo namespaceSerializerJsonInfo = (null == timerSerializer.getNamespaceSerializer())
                    ? null
                    : buildJsonInfoBy(
                    timerSerializer.getNamespaceSerializer(),
                    OmniSerializerType.get(timerSerializer.getNamespaceSerializer().getClass()),
                    depth + DEPTH_INTERVAL);
            jsonInfo.setKeySerializer(keySerializerJsonInfo);
            jsonInfo.setNamespaceSerializer(namespaceSerializerJsonInfo);
            return jsonInfo;
        } else if (OmniSerializerType.ROW.equals(serializerType)) {
            try {
                RowDataSerializer rowDataSerializer = (RowDataSerializer) typeSerializer;
                LogicalType[] types = ReflectionUtils.retrievePrivateField(rowDataSerializer, TYPE_SERIALIZER_PRIVATE_KEY_TYPES);
                RowType rowType = OmniStateSerializerUtils.getRowType(types);
                JsonNode logicalType = OmniStateSerializerUtils.logicalTypeJsonSerialize(rowType);
                jsonInfo.setLogicalType(logicalType);
                return jsonInfo;
            } catch (Exception e) {
                LOG.error("method : buildJsonInfoBy -> build row exception", e);
                throw new GeneralRuntimeException(e);
            }
        } else if (OmniSerializerType.BINARY_ROW.equals(serializerType)) {
            BinaryRowDataSerializer binaryRowDataSerializer = (BinaryRowDataSerializer) typeSerializer;
            List<String> inputTypes = binaryRowDataSerializer.getInputTypes();
            if (!CollectionUtil.isNullOrEmpty(inputTypes)) {
                jsonInfo.setFields(inputTypes);
            } else {
                // just for arity
                int arity = binaryRowDataSerializer.getArity() > 0 ? binaryRowDataSerializer.getArity() : 0;
                jsonInfo.setFields(Collections.nCopies(arity, SC.EMPTY));
            }
            return jsonInfo;
        } else if (OmniSerializerType.EXTERNAL.equals(serializerType)) {
            try {
                ExternalSerializer<?, ?> externalSerializer = (ExternalSerializer<?, ?>) typeSerializer;

                JsonNode logicalType = OmniStateSerializerUtils.logicalTypeJsonSerialize(externalSerializer.getDataType().getLogicalType());
                jsonInfo.setLogicalType(logicalType);

                jsonInfo.setClazz(externalSerializer.getDataType().getConversionClass().getName());
                jsonInfo.setSerializerAttributes(
                        OmniSerializerAttributesInfo.ofExternal(externalSerializer.isInternalInput(),
                                externalSerializer.getDataType().getConversionClass().getName()));

                TypeSerializer<?> internalSerializer = ReflectionUtils.retrievePrivateField(externalSerializer, TYPE_SERIALIZER_PRIVATE_KEY_INTERNAL_SERIALIZERS);
                if (internalSerializer != null) {
                    jsonInfo.setValueSerializer(buildJsonInfoBy(internalSerializer, OmniSerializerType.get(internalSerializer.getClass()), depth + DEPTH_INTERVAL));
                }
                return jsonInfo;
            } catch (Exception e) {
                LOG.error("method : buildJsonInfoBy -> build external exception", e);
                throw new GeneralRuntimeException(e);
            }
        } else {
            LOG.warn("method : buildJsonInfoBy -> unsupported serializerType : {}, typeSerializer : {}", serializerType.name(), typeSerializer.getClass().getName());
        }

        return null;
    }

    protected boolean check(String stateTableName, OmniNativeSerializerJsonInfo info) {
        return StringUtils.isNotEmpty(stateTableName) && null != info;
    }

    protected boolean check(TypeSerializer<?> typeSerializer, OmniSerializerType serializerType) {
        return null != typeSerializer && null != serializerType;
    }


    public abstract StateDescriptor<?, ?> buildDescriptorBy(String stateTableName, OmniNativeSerializerJsonInfo info);

    public OmniSerializerJsonInfo buildSerializerJsonBy(TypeSerializer<?> typeSerializer, OmniSerializerType serializerType) {
        if (!check(typeSerializer, serializerType)) {
            return null;
        }
        return buildJsonInfoBy(typeSerializer, serializerType, DEPTH_START);
    }
}
