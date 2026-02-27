package com.huawei.omniruntime.flink.runtime.api.state.serializer.factory.parse;

import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.SC;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums.OmniSerializerType;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniNativeSerializerJsonInfo;
import com.huawei.omniruntime.flink.runtime.api.state.serializer.model.info.OmniSerializerJsonInfo;
import com.huawei.omniruntime.flink.runtime.metrics.exception.GeneralRuntimeException;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.VoidNamespaceTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            return TypeExtractor.createTypeInfo(info.getElementTypeClazz());
        } else if (OmniSerializerType.VOID_NAMESPACE.equals(info.getSerializerType())) {
            return new VoidNamespaceTypeInfo();
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
        if (serializerType.isBasic()) {
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
