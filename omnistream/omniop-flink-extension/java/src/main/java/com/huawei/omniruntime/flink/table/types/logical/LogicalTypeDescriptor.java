/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package com.huawei.omniruntime.flink.table.types.logical;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.json.JSONObject;

/**
 * LogicalTypeDescriptor
 *
 * @since 2025-04-27
 */
public class LogicalTypeDescriptor {
    /**
     * varchar
     */
    public static final String VARCHAR = "VARCHAR";

    /**
     * bigInt
     */
    public static final String BIGINT = "BIGINT";

    // public static String TIMESTAMP_WITH_LOCAL_TIME_ZONE = "TIMESTAMP_WITH_LOCAL_TIME_ZONE";
    /**
     * timeStamp
     */
    public static final String TIMESTAMP = "TIMESTAMP";

    public static final String TIMESTAMP_LTZ = "TIMESTAMP_LTZ";

    /**
     * unsolved
     */
    public static final String UNRESOLVED = "UNRESOLVED";

    public static final String DECIMAL = "DECIMAL";

    public static final String BOOLEAN = "BOOLEAN";

    public static final String INTEGER = "INTEGER";

    public static final String CHAR = "CHAR";

    public static final String DOUBLE = "DOUBLE";

    // the type format should be
    //  { type : "typename",
    //    kind : "logic"   // logic are specific for sql type or subtype
    //    isNull: yes or no
    //    other attribute of type
    // eg.  length: number   // varchar
    // e.g. precision: number //  timestamp precision
    /**
     * createBasicTypeObject
     *
     * @param typeName typeName
     * @param isNull isNull
     * @return JSONObject
     */
    public static JSONObject createBasicTypeObject(String typeName, boolean isNull) {
        JSONObject data = new JSONObject();
        String kindStr = "logical";
        data.put("kind", kindStr);
        data.put("type", typeName);
        data.put("isNull", isNull);
        return data;
    }

    /**
     * createJSONDescriptorBigIntType
     *
     * @param bigIntType bigIntType
     * @return JSONObject
     */
    public static JSONObject createJSONDescriptorBigIntType(BigIntType bigIntType) {
        return createBasicTypeObject(BIGINT, bigIntType.isNullable());
    }

    /**
     * createJSONDescriptorVarCharType
     *
     * @param varCharType varCharType
     * @return JSONObject
     */
    public static JSONObject createJSONDescriptorVarCharType(VarCharType varCharType) {
        JSONObject typeDescriptor = createBasicTypeObject(VARCHAR, varCharType.isNullable());
        typeDescriptor.put("length", varCharType.getLength());
        return typeDescriptor;
    }

    /**
     * createJSONDescriptorTimestampType
     *
     * @param timestampType timestampType
     * @return JSONObject
     */
    public static JSONObject createJSONDescriptorTimestampType(TimestampType timestampType) {
        JSONObject typeDescriptor = createBasicTypeObject(TIMESTAMP, timestampType.isNullable());
        TimestampKind timestampKind = timestampType.getKind();
        typeDescriptor.put("timestampKind", timestampType.getKind().ordinal());
        typeDescriptor.put("precision", timestampType.getPrecision());
        return typeDescriptor;
    }

    public static JSONObject createJSONDescriptorTimestampLtzType(LocalZonedTimestampType timestampType) {
        JSONObject typeDescriptor = createBasicTypeObject(TIMESTAMP_LTZ, timestampType.isNullable());
        typeDescriptor.put("precision", timestampType.getPrecision());
        return typeDescriptor;
    }

    /**
     * createJSONDescriptorUNRESOLVED
     *
     * @param type type
     * @return JSONObject
     */
    public static JSONObject createJSONDescriptorUNRESOLVED(LogicalType type) {
        JSONObject typeDescriptor = createBasicTypeObject(UNRESOLVED, type.isNullable());
        typeDescriptor.put("meta", type);
        return typeDescriptor;
    }

    public static JSONObject createJSONDescriptorDecimalType(DecimalType decimalType) {
        JSONObject typeDescriptor = createBasicTypeObject(DECIMAL, decimalType.isNullable());
        typeDescriptor.put("precision", decimalType.getPrecision());
        typeDescriptor.put("scale", decimalType.getScale());
        return typeDescriptor;
    }

    public static JSONObject createJSONDescriptorBooleanType(BooleanType booleanType) {
        return createBasicTypeObject(BOOLEAN, booleanType.isNullable());
    }

    public static JSONObject createJSONDescriptorIntType(IntType intType) {
        return createBasicTypeObject(INTEGER, intType.isNullable());
    }

    public static JSONObject createJSONDescriptorCharType(CharType charType) {
        JSONObject typeDescriptor = createBasicTypeObject(CHAR, charType.isNullable());
        typeDescriptor.put("length", charType.getLength());
        return typeDescriptor;
    }

    public static JSONObject createJSONDescriptorDoubleType(DoubleType doubleType) {
        return createBasicTypeObject(DOUBLE, doubleType.isNullable());
    }

    /**
     * 生成 csvSpec.fields[] 用的类型描述，native 侧按同样的 logicalType 名解析；
     * 不支持的类型返回 null。
     */
    public static JSONObject createCsvFieldDescriptor(LogicalType type) {
        if (type instanceof BigIntType) {
            return createJSONDescriptorBigIntType((BigIntType) type);
        }
        if (type instanceof VarCharType) {
            return createJSONDescriptorVarCharType((VarCharType) type);
        }
        if (type instanceof CharType) {
            return createJSONDescriptorCharType((CharType) type);
        }
        if (type instanceof TimestampType) {
            return createJSONDescriptorTimestampType((TimestampType) type);
        }
        if (type instanceof LocalZonedTimestampType) {
            return createJSONDescriptorTimestampLtzType((LocalZonedTimestampType) type);
        }
        if (type instanceof DecimalType) {
            return createJSONDescriptorDecimalType((DecimalType) type);
        }
        if (type instanceof BooleanType) {
            return createJSONDescriptorBooleanType((BooleanType) type);
        }
        if (type instanceof IntType) {
            return createJSONDescriptorIntType((IntType) type);
        }
        if (type instanceof DoubleType) {
            return createJSONDescriptorDoubleType((DoubleType) type);
        }
        return null;
    }
}


/**
 * TBD all logical type should have a name for TDD JSON
 * <p>
 * CHAR(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.CHARACTER_STRING),
 * <p>
 * VARCHAR(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.CHARACTER_STRING),
 * <p>
 * BOOLEAN(LogicalTypeFamily.PREDEFINED),
 * <p>
 * BINARY(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.BINARY_STRING),
 * <p>
 * VARBINARY(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.BINARY_STRING),
 * <p>
 * DECIMAL(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.EXACT_NUMERIC),
 * <p>
 * TINYINT(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.INTEGER_NUMERIC,
 * LogicalTypeFamily.EXACT_NUMERIC),
 * <p>
 * SMALLINT(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.INTEGER_NUMERIC,
 * LogicalTypeFamily.EXACT_NUMERIC),
 * <p>
 * INTEGER(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.INTEGER_NUMERIC,
 * LogicalTypeFamily.EXACT_NUMERIC),
 * <p>
 * BIGINT(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.INTEGER_NUMERIC,
 * LogicalTypeFamily.EXACT_NUMERIC),
 * <p>
 * FLOAT(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.APPROXIMATE_NUMERIC),
 * <p>
 * DOUBLE(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.NUMERIC,
 * LogicalTypeFamily.APPROXIMATE_NUMERIC),
 * <p>
 * DATE(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.DATETIME),
 * <p>
 * TIME_WITHOUT_TIME_ZONE(
 * LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.DATETIME, LogicalTypeFamily.TIME),
 * <p>
 * TIMESTAMP_WITHOUT_TIME_ZONE(
 * LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.DATETIME, LogicalTypeFamily.TIMESTAMP),
 * <p>
 * TIMESTAMP_WITH_TIME_ZONE(
 * LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.DATETIME, LogicalTypeFamily.TIMESTAMP),
 * <p>
 * TIMESTAMP_WITH_LOCAL_TIME_ZONE(
 * LogicalTypeFamily.PREDEFINED,
 * LogicalTypeFamily.DATETIME,
 * LogicalTypeFamily.TIMESTAMP,
 * LogicalTypeFamily.EXTENSION),
 * <p>
 * INTERVAL_YEAR_MONTH(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.INTERVAL),
 * <p>
 * INTERVAL_DAY_TIME(LogicalTypeFamily.PREDEFINED, LogicalTypeFamily.INTERVAL),
 * <p>
 * ARRAY(LogicalTypeFamily.CONSTRUCTED, LogicalTypeFamily.COLLECTION),
 * <p>
 * MULTISET(LogicalTypeFamily.CONSTRUCTED, LogicalTypeFamily.COLLECTION),
 * <p>
 * MAP(LogicalTypeFamily.CONSTRUCTED, LogicalTypeFamily.EXTENSION),
 * <p>
 * ROW(LogicalTypeFamily.CONSTRUCTED),
 * <p>
 * DISTINCT_TYPE(LogicalTypeFamily.USER_DEFINED),
 * <p>
 * STRUCTURED_TYPE(LogicalTypeFamily.USER_DEFINED),
 * <p>
 * NULL(LogicalTypeFamily.EXTENSION),
 * <p>
 * RAW(LogicalTypeFamily.EXTENSION),
 * <p>
 * SYMBOL(LogicalTypeFamily.EXTENSION),
 * <p>
 * UNRESOLVED(LogicalTypeFamily.EXTENSION);
 */
