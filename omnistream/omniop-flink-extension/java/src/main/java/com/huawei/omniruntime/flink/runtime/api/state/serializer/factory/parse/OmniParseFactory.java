package com.huawei.omniruntime.flink.runtime.api.state.serializer.factory.parse;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.SC;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerType;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniNativeSerializerJsonInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniSerializerJsonInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.type.TimerTypeInfo;
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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonDeserializer;
import org.apache.flink.table.gateway.rest.serde.LogicalTypeJsonSerializer;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * OmniParseFactory
 *
 */

public abstract class OmniParseFactory {
    private static final Logger LOG = LoggerFactory.getLogger(OmniParseFactory.class);

    public static final String TYPE_SERIALIZER_PRIVATE_KEY_CLAZZ = "clazz";
    public static final String TYPE_SERIALIZER_PRIVATE_KEY_FIELDS = "fields";
    public static final String TYPE_SERIALIZER_PRIVATE_KEY_FIELD_SERIALIZERS = "fieldSerializers";

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
                    factory = new OmniParseValueFactory();
                    break;
                case UNKNOW:
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
            return Types.POJO(info.getElementTypeClazz());
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
                Class<?> tupleClass = info.getElementTypeClazz();
                if (tupleClass == null) {
                    // C++ 端漏传 element_type 时按 arity 兜底到 TupleN.class
                    tupleClass = Tuple.getTupleClass(fieldTypes.length);
                }
                return new TupleTypeInfo(tupleClass, fieldTypes);
            }
            // 兼容老 JSON：无 fieldSerializers 时退回基于 raw class 的解析
            return TypeExtractor.createTypeInfo(info.getElementTypeClazz());
        } else if (OmniSerializerType.VOID_NAMESPACE.equals(info.getSerializerType())) {
            return new VoidNamespaceTypeInfo();
        } else if (OmniSerializerType.TIMER.equals(info.getSerializerType())) {
            OmniNativeSerializerJsonInfo keySerializerInfo = info.getKeySerializer();
            OmniNativeSerializerJsonInfo namespaceSerializerInfo = info.getNamespaceSerializer();
            TypeInformation<?> keyTypeInfo =  (null == keySerializerInfo)
                    ? Types.STRING : buildTypeInformationBy(keySerializerInfo, depth + DEPTH_INTERVAL);
            TypeInformation<?> namespaceTypeInfo =  (null == namespaceSerializerInfo)
                    ? new VoidNamespaceTypeInfo() : buildTypeInformationBy(namespaceSerializerInfo, depth + DEPTH_INTERVAL);
            return new TimerTypeInfo<>(keyTypeInfo, namespaceTypeInfo);
        } else if (OmniSerializerType.ROW.equals(info.getSerializerType())) {
            try {
                LogicalTypeJsonDeserializer deserializer = new LogicalTypeJsonDeserializer();
                RowType rowType = (RowType) deserializer.deserialize(info.getLogicalType(), null);
                return InternalTypeInfo.of(rowType);
            } catch (Exception e) {
                LOG.error("method : buildTypeInformationBy -> build row exception", e);
                throw new GeneralRuntimeException(e);
            }
        }

        return null;
    }

    protected OmniSerializerJsonInfo buildJsonInfoBy(TypeSerializer<?> typeSerializer, OmniSerializerType serializerType, int depth) {
        if (depth > DEPTH_MAX) {
            throw new GeneralRuntimeException(String.format("max recursion depth (%s) exceeded. Input may be malformed or malicious.", DEPTH_MAX));
        }
        if (null == typeSerializer || null == serializerType) {
            return null;
        }
        OmniSerializerJsonInfo jsonInfo = new OmniSerializerJsonInfo();
        jsonInfo.setSerializerName(typeSerializer.getClass().getName());
        jsonInfo.setSerializerInstanceClazz(typeSerializer.createInstance().getClass().getName());
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
            return jsonInfo;
        } else if (OmniSerializerType.VOID_NAMESPACE.equals(serializerType)) {
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
                RowType rowType = rowDataSerializer.getRowType();

                LogicalTypeJsonSerializer serializer = new LogicalTypeJsonSerializer();
                StringWriter stringWriter = new StringWriter();
                try (JsonGenerator jsonGenerator = JsonHelper.getObjectMapper().createGenerator(stringWriter)) {
                    serializer.serialize(rowType, jsonGenerator, null);
                    jsonGenerator.flush();
                }

                String jsonString = stringWriter.toString();
                JsonNode logicType = JsonHelper.fromJson(jsonString, JsonNode.class);
                jsonInfo.setLogicalType(logicType);
                return jsonInfo;
            } catch (Exception e) {
                LOG.error("method : buildJsonInfoBy -> build row exception", e);
                throw new GeneralRuntimeException(e);
            }
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
        OmniSerializerJsonInfo jsonInfo = buildJsonInfoBy(typeSerializer, serializerType, DEPTH_START);
        if (null == jsonInfo) {
            return null;
        }
        return jsonInfo;
    }
}
