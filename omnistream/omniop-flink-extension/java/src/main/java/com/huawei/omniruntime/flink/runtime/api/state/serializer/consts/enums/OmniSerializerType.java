package com.huawei.omniruntime.flink.runtime.api.state.serializer.consts.enums;

import org.apache.flink.api.common.typeutils.base.*;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Date;

/**
 * OmniSerializerType
 *
 */
public enum OmniSerializerType {
    VOID(OmniSerializerCategory.BASIC, Void.class, VoidSerializer.class, -1),
    STRING(OmniSerializerCategory.BASIC, String.class, StringSerializer.class, 8),
    BYTE(OmniSerializerCategory.BASIC, Byte.class, ByteSerializer.class, -1),
    BOOLEAN(OmniSerializerCategory.BASIC, Boolean.class, BooleanSerializer.class, -1),
    SHORT(OmniSerializerCategory.BASIC, Short.class, ShortSerializer.class, -1),
    INT(OmniSerializerCategory.BASIC, Integer.class, IntSerializer.class, 4),
    LONG(OmniSerializerCategory.BASIC, Long.class, LongSerializer.class, 3),
    FLOAT(OmniSerializerCategory.BASIC, Float.class, FloatSerializer.class, -1),
    DOUBLE(OmniSerializerCategory.BASIC, Double.class, DoubleSerializer.class, 5),
    CHAR(OmniSerializerCategory.BASIC, Character.class, CharSerializer.class, -1),
    BIG_DEC(OmniSerializerCategory.BASIC, BigDecimal.class, BigDecSerializer.class, -1),
    BIG_INT(OmniSerializerCategory.BASIC, BigInteger.class, BigIntSerializer.class, 2),
    INSTANT(OmniSerializerCategory.BASIC, Instant.class, InstantSerializer.class, -1),
    DATE(OmniSerializerCategory.DATE, Date.class, DateSerializer.class, -1),


    LIST(OmniSerializerCategory.LIST, null, ListSerializer.class, 1),
    MAP(OmniSerializerCategory.MAP, null, MapSerializer.class, 6),
    POJO(OmniSerializerCategory.LIST, null, PojoSerializer.class, 7),
    TUPLE(OmniSerializerCategory.TUPLE, null, TupleSerializer.class, -1),
    VOID_NAMESPACE(OmniSerializerCategory.VOID_NAMESPACE, VoidNamespace.class, VoidNamespaceSerializer.class, -1),

    UNKNOW(OmniSerializerCategory.UNKNOWN, null, null, 0);

    private final OmniSerializerCategory category;
    private final Class<?> clazz;
    private final Class<?> serializerClazz;
    private final int code;

    OmniSerializerType(OmniSerializerCategory category, Class<?> clazz, Class<?> serializerClazz, int code) {
        this.category = category;
        this.clazz = clazz;
        this.serializerClazz = serializerClazz;
        this.code = code;
    }

    public Class<?> getClazz() {
        return this.clazz;
    }

    public int getCode() {
        return this.code;
    }

    public Class<?> getSerializerClazz() {
        return this.serializerClazz;
    }

    public boolean equals(int code) {
        return this.code == code;
    }

    public boolean equals(Class<?> serializerClazz) {
        if (null == serializerClazz) {
            return false;
        }
        if (null == this.serializerClazz) {
            return false;
        }
        return this.serializerClazz.getName().equalsIgnoreCase(serializerClazz.getName());
    }

    public boolean isBasic() {
        return OmniSerializerCategory.BASIC.equals(this.category);
    }

    public static OmniSerializerType get(int code) {
        for (OmniSerializerType item : OmniSerializerType.values()) {
            if (item.equals(code)) {
                return item;
            }
        }

        return null;
    }

    public static OmniSerializerType get(Class<?> serializerClazz) {
        if (null == serializerClazz) {
            return null;
        }

        for (OmniSerializerType item : OmniSerializerType.values()) {
            if (item.equals(serializerClazz)) {
                return item;
            }
        }

        return null;
    }
}
