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

package org.apache.flink.table.planner.plan.nodes.exec.util;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Sarg;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class RexNodeUtil {
    // native 的 TIME/TIMESTAMP 以 int64 毫秒存储，因此基于它们的算术运算需要用该值
    // 按天取模回绕。24 * 60 * 60 * 1000。
    private static final long MILLIS_PER_DAY = 86400000L;
    // native 的 int64-毫秒表示最多只能容纳精度 <= 3（毫秒）。TIME/TIMESTAMP 精度为 4-9
    // （微秒/纳秒）时会丢数据，因此这里直接拒绝，交还给 Flink 处理对应 RexNode。
    private static final int MAX_NATIVE_TIME_PRECISION = 3;
    // datetime + interval 的 native 函数名。年月间隔与天时间间隔需要不同的 kernel：
    // 一个月没有固定的毫秒宽度，无法复用天时间（纯毫秒）的实现路径。
    // 这些字符串必须与 Register*.cpp 中的注册名完全一致。
    private static final String DATETIME_PLUS_YEAR_MONTH = "datetime_plus_year_month";
    private static final String DATETIME_PLUS_DAY_TIME = "datetime_plus_day_time";
    private static final String DATETIME_MINUS_DAY_TIME = "datetime_minus_day_time";
    private static final String TIME_PLUS_YEAR_MONTH = "time_plus_year_month";
    private static final int MAX_NATIVE_TIMESTAMP_PRECISION = 3;
    public static final Map<String, Integer> RexTypeToIdMap = new HashMap<>();
    public static final Map<String, SpecialExprType> specialOperatorMap = new HashMap<>();
    public static final Map<SpecialExprType, String> simpleFunctionNameMap = new HashMap<>();
    public static final Map<String, SpecialExprType> udfOperatorMap = new HashMap<>();
    public static final Map<String, UnaryExprType> unaryOperatorMap = new HashMap<>();
    public static final Map<String, BinaryExprType> binaryOperatorMap = new HashMap<>();
    // Strategy registry: maps a SpecialExprType to the handler that builds its JSON.
    // Handlers live in per-category classes (LogicExprHandlers, JsonExprHandlers,
    // DateTimeExprHandlers, StringExprHandlers, MathExprHandlers) plus the core
    // structural handlers below.
    // Each category self-registers via its register() method (called in the static block),
    // so adding a function within a category touches only that category's file instead of
    // this shared class (reduces multi-developer merge conflicts).
    public static final Map<SpecialExprType, SpecialExprHandler> specialHandlerMap = new HashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(RexNodeUtil.class);
    public static HashMap<Integer, Integer> accessIndexMap = new HashMap<>();

    static {
        // This map converts Calcite RexNode SqlTypeName to OmniType Id
        RexTypeToIdMap.put("INT", 1);
        RexTypeToIdMap.put("INTEGER", 1);
        RexTypeToIdMap.put("BIGINT", 2);
        RexTypeToIdMap.put("DOUBLE", 3);
        RexTypeToIdMap.put("BOOLEAN", 4);
        RexTypeToIdMap.put("TINYINT", 5);
        RexTypeToIdMap.put("SMALLINT", 5);
        RexTypeToIdMap.put("DECIMAL", 6);
        RexTypeToIdMap.put("DECIMAL128", 7);
        RexTypeToIdMap.put("DATE", 8);
        RexTypeToIdMap.put("TIME", 10);
        RexTypeToIdMap.put("TIMESTAMP", 12);
        RexTypeToIdMap.put("INTERVAL_MONTH", 13);
        RexTypeToIdMap.put("INTERVAL_DAY", 14);
        RexTypeToIdMap.put("INTERVAL_SECOND", 14);
        RexTypeToIdMap.put("VARCHAR", 15);
        RexTypeToIdMap.put("CHAR", 16);
        RexTypeToIdMap.put("ROW", 17);
        RexTypeToIdMap.put("INVALID", 18);
        RexTypeToIdMap.put("TIME_WITHOUT_TIME_ZONE", 19); // TODO: Is this the same as TIME?
        RexTypeToIdMap.put("VARBINARY", 20);
        RexTypeToIdMap.put("BINARY", 20);
        RexTypeToIdMap.put("TIMESTAMP_WITHOUT_TIME_ZONE", 22);
        RexTypeToIdMap.put("TIMESTAMP_TZ", 23); // TIMESTAMP_WITH_TIMEZONE
        RexTypeToIdMap.put("TIMESTAMP_WITH_LOCAL_TIME_ZONE", 24);
        RexTypeToIdMap.put("ARRAY", 30);
        RexTypeToIdMap.put("MULTISET", 25);
        RexTypeToIdMap.put("MAP", 31);
        RexTypeToIdMap.put("ROW", 32);
    }

    static {
        // Core structural operators handled in this class (see the handlers at the bottom).
        specialOperatorMap.put("CASE", SpecialExprType.SWITCH);
        specialOperatorMap.put("SEARCH", SpecialExprType.SEARCH);
        specialOperatorMap.put("HASH_CODE", SpecialExprType.HASH_CODE);
        specialOperatorMap.put("CAST", SpecialExprType.CAST);
        specialOperatorMap.put("AND", SpecialExprType.AND);
        specialOperatorMap.put("OR", SpecialExprType.OR);
        specialOperatorMap.put("NOT", SpecialExprType.NOT);
        specialOperatorMap.put("IF", SpecialExprType.IF);
        specialOperatorMap.put("COALESCE", SpecialExprType.COALESCE);
        // IFNULL reuses the COALESCE native path (equivalent to 2-arg COALESCE)
        specialOperatorMap.put("IFNULL", SpecialExprType.COALESCE);
        specialOperatorMap.put("TYPEOF", SpecialExprType.TYPEOF);
        specialOperatorMap.put("EXP", SpecialExprType.EXP);
        specialOperatorMap.put("LOG", SpecialExprType.LOG);
        specialOperatorMap.put("LOG2", SpecialExprType.LOG2);
        specialOperatorMap.put("LOG10", SpecialExprType.LOG10);
        specialOperatorMap.put("REGEXP", SpecialExprType.REGEXP);
        specialOperatorMap.put("RLIKE", SpecialExprType.REGEXP);
        specialOperatorMap.put("TIMESTAMP", SpecialExprType.TIMESTAMP);
        specialOperatorMap.put("SQRT", SpecialExprType.SQRT);
        specialOperatorMap.put("IS_DIGIT", SpecialExprType.IS_DIGIT);
        specialOperatorMap.put("ENCODE", SpecialExprType.ENCODE);
        specialOperatorMap.put("DECODE", SpecialExprType.DECODE);
        specialOperatorMap.put("TIMESTAMPADD", SpecialExprType.TIMESTAMPADD);
        specialOperatorMap.put("REGEXP_REPLACE", SpecialExprType.REGEXP_REPLACE);
        specialOperatorMap.put("TIME", SpecialExprType.TIME);
    }

    static {
        // Map SpecialExprType to the OmniOperatorJIT C++ registered function_name.
        // Used by the generic FUNCTION case that forwards all operands as arguments.
        simpleFunctionNameMap.put(SpecialExprType.EXP, "exp");
        simpleFunctionNameMap.put(SpecialExprType.LOG2, "log2");
        simpleFunctionNameMap.put(SpecialExprType.LOG10, "log10");
        simpleFunctionNameMap.put(SpecialExprType.REGEXP, "RLike");
        simpleFunctionNameMap.put(SpecialExprType.SQRT, "sqrt");
        simpleFunctionNameMap.put(SpecialExprType.IS_DIGIT, "is_digit");
        simpleFunctionNameMap.put(SpecialExprType.TIME, "time");
    }

    static {
        unaryOperatorMap.put("+", UnaryExprType.POSITIVE);
        unaryOperatorMap.put("-", UnaryExprType.NEGATION);
        unaryOperatorMap.put("NOT", UnaryExprType.NOT);
    }

    static {
        binaryOperatorMap.put("OR", BinaryExprType.OR);
        binaryOperatorMap.put("AND", BinaryExprType.AND);
        binaryOperatorMap.put("+", BinaryExprType.ADD);
        binaryOperatorMap.put("-", BinaryExprType.SUBTRACT);
        binaryOperatorMap.put("*", BinaryExprType.MULTIPLY);
        binaryOperatorMap.put("/", BinaryExprType.DIVIDE);
        binaryOperatorMap.put("MOD", BinaryExprType.MODULUS);
        binaryOperatorMap.put(">", BinaryExprType.GREATER_THAN);
        binaryOperatorMap.put(">=", BinaryExprType.GREATER_THAN_OR_EQUAL);
        binaryOperatorMap.put("<", BinaryExprType.LESS_THAN);
        binaryOperatorMap.put("<=", BinaryExprType.LESS_THAN_OR_EQUAL);
        binaryOperatorMap.put("=", BinaryExprType.EQUAL);
        binaryOperatorMap.put("<>", BinaryExprType.NOT_EQUAL);
    }

    static {
        // Register the core structural handlers kept in this class.
        specialHandlerMap.put(SpecialExprType.SWITCH, RexNodeUtil::handleSwitch);
        specialHandlerMap.put(SpecialExprType.IF, RexNodeUtil::handleSwitch);
        specialHandlerMap.put(SpecialExprType.COALESCE, RexNodeUtil::handleCoalesce);
        specialHandlerMap.put(SpecialExprType.SEARCH, RexNodeUtil::handleSearch);
        specialHandlerMap.put(SpecialExprType.HASH_CODE, RexNodeUtil::handleHashCode);
        specialHandlerMap.put(SpecialExprType.AND, RexNodeUtil::handleAnd);
        specialHandlerMap.put(SpecialExprType.OR, RexNodeUtil::handleOr);
        specialHandlerMap.put(SpecialExprType.CAST, RexNodeUtil::handleCast);
        specialHandlerMap.put(SpecialExprType.TYPEOF, RexNodeUtil::handleTypeOf);
        specialHandlerMap.put(SpecialExprType.EXP, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.LOG, RexNodeUtil::handleLogFunction);
        specialHandlerMap.put(SpecialExprType.LOG2, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.LOG10, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.REGEXP, RexNodeUtil::handleSimpleFunction);
        specialHandlerMap.put(SpecialExprType.TIMESTAMP, RexNodeUtil::handleTimestamp);
        specialHandlerMap.put(SpecialExprType.SQRT, RexNodeUtil::handleSimpleFunction);

        // Category handlers self-register their operator names, native function names and
        // handlers. Adding a function within a category touches only that category's file
        // (plus the shared SpecialExprType enum). Adding a new category = one line here.
        LogicExprHandlers.register();
        StringExprHandlers.register();
        DateTimeExprHandlers.register();
        MathExprHandlers.register();
        JsonExprHandlers.register();
        // LOG2 handler
        specialHandlerMap.put(SpecialExprType.LOG2, RexNodeUtil::handleSimpleFunction);
        // IS_DIGIT uses the shared FUNCTION-forwarding handler
        specialHandlerMap.put(SpecialExprType.IS_DIGIT, RexNodeUtil::handleSimpleFunction);
        // ENCODE handler
        specialHandlerMap.put(SpecialExprType.ENCODE, RexNodeUtil::handleEncode);
        // DECODE handler
        specialHandlerMap.put(SpecialExprType.DECODE, RexNodeUtil::handleDecode);
        // TIMESTAMPADD handler
        specialHandlerMap.put(SpecialExprType.TIMESTAMPADD, RexNodeUtil::handleTimestampAdd);
        // REGEXP_REPLACE handler
        specialHandlerMap.put(SpecialExprType.REGEXP_REPLACE, RexNodeUtil::handleRegexpReplace);
        // TIME handler
        specialHandlerMap.put(SpecialExprType.TIME, RexNodeUtil::handleSimpleFunction);
    }

    private static <T> T resolveOperatorType(Map<String, T> operatorMap, String operatorName) {
        T operatorType = operatorMap.get(operatorName);
        if (operatorType != null) {
            return operatorType;
        }
        return operatorMap.get(operatorName.toUpperCase(Locale.ROOT));
    }

    /**
     * Strategy interface for translating a special RexCall into its OmniOperator JSON map.
     * Implemented by the {@code handleXxx} methods (in this class and the per-category handler
     * classes) and wired up in {@link #specialHandlerMap}. This replaces the former monolithic
     * switch-case so that adding a new special expression does not require editing a shared block.
     */
    @FunctionalInterface
    public interface SpecialExprHandler {
        Map<String, Object> handle(RexCall rexCall, List<RexNode> operands,
                Map<String, Object> jsonMap, SpecialExprType specialType);
    }

    public enum OperatorExprType {
        FIELD_REFERENCE,
        FIELD_ACCESS, // we can parse it, but currently we dont support it in operator-level operations.
        LITERAL,
        UNARY,
        BINARY,
        SPECIAL,
        INVALID
    }

    public enum UnaryExprType {
        POSITIVE,
        NEGATION,
        NOT
    }


    public enum BinaryExprType {
        // binary logical operators
        OR,
        AND,
        // Arithmetic
        ADD,
        SUBTRACT,
        MULTIPLY,
        DIVIDE,
        MODULUS,
        // Comparison
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        EQUAL,
        NOT_EQUAL
    }

    public enum SpecialExprType {
        SWITCH,
        REGEXP_EXTRACT,
        SEARCH,
        LOWER,
        HASH_CODE,
        SPLIT_INDEX,
        FROM_BASE64,
        CHAR_LENGTH,
        IS_NOT_NULL,
        PROCTIME,
        EXTRACT,
        DATE_FORMAT,
        TO_TIMESTAMP_LTZ,
        TO_TIMESTAMP,
        COUNT_CHAR,
        CAST,
        OTHERS,
        AND,
        OR,
        NOT,
        IF,
        COALESCE,
        JSON_VALUE,
        JSON_QUERY,
        JSON_EXISTS,
        JSON_SPLIT,
        JSON_STRING,
        JSON_ARRAY,
        JSON_OBJECT,
        CURRENT_TIMESTAMP,
        CURRENT_WATERMARK,
        DATE_ADD,
        FLOOR,
        LN,
        CEIL,
        IS_NULL,
        ROUND,
        GREATEST,
        LEAST,
        CONCAT,
        CONCAT_WS,
        REPLACE,
        SUBSTR,
        INSTR,
        PARSE_URL,
        UNIX_TIMESTAMP,
        FROM_UNIXTIME,
        IS_FALSE,
        IS_NOT_FALSE,
        LOCALTIME,
        LOCALTIMESTAMP,
        CURRENT_ROW_TIMESTAMP,
        CURRENT_DATE,
        NULLIF,
        IS_TRUE,
        IS_NOT_TRUE,
        LIKE,
        TO_DATE,
        RPAD,
        LPAD,
        REPEAT,
        OVERLAY,
        SINH,
        COS,
        COT,
        ASIN,
        ACOS,
        ATAN,
        ATAN2,
        COSH,
        DEGREES,
        SIGN,
        SIN,
        TAN,
        TANH,
        RADIANS,
        PI,
        E,
        RAND,
        RAND_INTEGER,
        UUID,
        BIN,
        HEX,
        TRUNCATE,
        IS_ALPHA,
        IS_DECIMAL,
        IS_JSON_VALUE,
        IS_JSON_SCALAR,
        IS_JSON_ARRAY,
        IS_JSON_OBJECT,
        TYPEOF,
        LEFT,
        RIGHT,
        STR_TO_MAP,
        CONVERT_TZ,
        ABS,
        UPPER,
        POSITION,
        TRIM,
        LTRIM,
        RTRIM,
        INITCAP,
        TO_BASE64,
        ASCII,
        LOCATE,
        REVERSE,
        POWER,
        CHR,
        SIMILAR_TO,
        EXP,
        LOG,
        LOG2,
        LOG10,
        REGEXP,
        TIMESTAMP,
        SQRT,
        IS_DIGIT,
        ENCODE,
        DECODE,
        TIMESTAMPADD,
        REGEXP_REPLACE,
        IN_SUBQUERY,
        NOT_IN_SUBQUERY,
        TIME,
        DATE
    }


    public static Object[] getExpression(RexNode inputExpr) {
        RexNode input = null;
        RexNode value = null;
        if (inputExpr instanceof RexCall) {
            RexCall call = (RexCall) inputExpr;
            LOG.info("Current RexCall name {}", call.getOperator().getName());
            if (binaryOperatorMap.containsKey(call.getOperator().getName())
                    && (binaryOperatorMap.get(call.getOperator().getName()) == BinaryExprType.EQUAL
                    || binaryOperatorMap.get(call.getOperator().getName()) == BinaryExprType.AND)) {
                input = call.operands.get(0);
                value = call.operands.get(1);
                return new Object[]{input, value};
            } else {
                LOG.info("Cannot parse the expression of a CASE operation");
            }
        } else {
            LOG.info("Cannot parse the expression of a CASE operation");
        }
        return new Object[]{input, value};
    }

    public static void setDataType(RexNode rexNode, Map<String, Object> jsonMap, String keyStr) {
        if (rexNode.getType().getSqlTypeName() == SqlTypeName.DECIMAL) {
            // check for decimal precision
            int precision = rexNode.getType().getPrecision();
            if (precision < 19) {
                jsonMap.put(keyStr, 6);
            } else {
                jsonMap.put(keyStr, 7);
            }
            jsonMap.put("precision", precision);
            jsonMap.put("scale", rexNode.getType().getScale());
        } else if (rexNode.getType().getSqlTypeName() == SqlTypeName.CHAR
                || rexNode.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
            int precision = rexNode.getType().getPrecision();
            jsonMap.put(keyStr, RexTypeToIdMap.get(rexNode.getType().getSqlTypeName().toString()));
            jsonMap.put("width", precision);
        } else if (rexNode.getType().getSqlTypeName() == SqlTypeName.BINARY
                || rexNode.getType().getSqlTypeName() == SqlTypeName.VARBINARY) {
            int precision = rexNode.getType().getPrecision();
            jsonMap.put(keyStr, RexTypeToIdMap.get(rexNode.getType().getSqlTypeName().toString()));
            jsonMap.put("width", precision);
        } else if (rexNode.getType().getSqlTypeName() == SqlTypeName.DATE) {
            // OmniStream carries DATE as INT (days since epoch): the operator-side
            // time expressions work in microseconds, which OmniStream does not parse,
            // so DATE/TIMESTAMP cross the boundary as OMNI_INT/OMNI_LONG.
            jsonMap.put(keyStr, RexTypeToIdMap.get("INT"));
        } else if (SqlTypeName.DATETIME_TYPES.contains(rexNode.getType().getSqlTypeName())) {
            jsonMap.put(keyStr, 2);
        } else if (SqlTypeName.YEAR_INTERVAL_TYPES.contains(rexNode.getType().getSqlTypeName())) {
            jsonMap.put(keyStr, RexTypeToIdMap.get("INTERVAL_MONTH"));
        } else if (SqlTypeName.DAY_INTERVAL_TYPES.contains(rexNode.getType().getSqlTypeName())) {
            jsonMap.put(keyStr, RexTypeToIdMap.get("INTERVAL_DAY"));
        } else {
            jsonMap.put(keyStr, RexTypeToIdMap.get(rexNode.getType().getSqlTypeName().toString()));
        }
    }

    private static boolean isTimestampType(RexNode rexNode) {
        SqlTypeName typeName = rexNode.getType().getSqlTypeName();
        return typeName == SqlTypeName.TIMESTAMP
                || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    private static boolean hasSupportedTimestampPrecision(RexNode rexNode) {
        int precision = rexNode.getType().getPrecision();
        return precision >= 0 && precision <= MAX_NATIVE_TIMESTAMP_PRECISION;
    }

    private static boolean isDayTimeIntervalType(RexNode rexNode) {
        return SqlTypeName.DAY_INTERVAL_TYPES.contains(rexNode.getType().getSqlTypeName());
    }

    private static boolean isIntervalType(RexNode rexNode) {
        return SqlTypeName.INTERVAL_TYPES.contains(rexNode.getType().getSqlTypeName());
    }

    private static RexLiteral getPureIntervalLiteral(RexNode rexNode) {
        if (rexNode instanceof RexLiteral && isIntervalType(rexNode)) {
            return (RexLiteral) rexNode;
        }
        if (!(rexNode instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) rexNode;
        if (call.getOperands().size() != 1
                || (call.getKind() != SqlKind.MINUS_PREFIX && call.getKind() != SqlKind.PLUS_PREFIX)) {
            return null;
        }
        RexNode operand = call.getOperands().get(0);
        return operand instanceof RexLiteral && isIntervalType(operand) ? (RexLiteral) operand : null;
    }

    private static Map<String, Object> tryBuildIntervalLiteralProjection(RexNode rexNode) {
        RexLiteral literal = getPureIntervalLiteral(rexNode);
        if (literal == null) {
            return null;
        }
        if (literal.isNull()) {
            LOG.info("NULL INTERVAL literals are outside the native literal projection scope: {}", rexNode);
            return invalidExpression();
        }
        Object value = literal.getValue2();
        if (!(value instanceof BigDecimal)) {
            LOG.info("Expected a normalized BigDecimal for INTERVAL literal {}, but got {}", rexNode,
                    value == null ? "null" : value.getClass().getName());
            return invalidExpression();
        }

        BigDecimal normalizedValue = (BigDecimal) value;
        if (rexNode instanceof RexCall && rexNode.getKind() == SqlKind.MINUS_PREFIX) {
            normalizedValue = normalizedValue.negate();
        }

        Map<String, Object> literalMap = new LinkedHashMap<>();
        literalMap.put("exprType", OperatorExprType.LITERAL.name());
        setDataType(literal, literalMap, "dataType");
        literalMap.put("isNull", false);
        try {
            if (SqlTypeName.YEAR_INTERVAL_TYPES.contains(literal.getType().getSqlTypeName())) {
                literalMap.put("value", normalizedValue.intValueExact());
            } else {
                literalMap.put("value", normalizedValue.longValueExact());
            }
            return literalMap;
        } catch (ArithmeticException e) {
            LOG.info("INTERVAL literal is outside the exact native range: {}", rexNode, e);
            return invalidExpression();
        }
    }

    private static Long getDayTimeIntervalLiteralMillis(RexNode rexNode) {
        RexNode literalNode = rexNode;
        boolean negate = false;
        if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            if (call.getOperands().size() != 1
                    || (call.getKind() != SqlKind.MINUS_PREFIX && call.getKind() != SqlKind.PLUS_PREFIX)) {
                return null;
            }
            literalNode = call.getOperands().get(0);
            negate = call.getKind() == SqlKind.MINUS_PREFIX;
        }
        if (!(literalNode instanceof RexLiteral) || !isDayTimeIntervalType(literalNode)) {
            return null;
        }
        Object value = ((RexLiteral) literalNode).getValue2();
        if (!(value instanceof BigDecimal)) {
            return null;
        }
        try {
            BigDecimal normalizedValue = negate ? ((BigDecimal) value).negate() : (BigDecimal) value;
            return normalizedValue.longValueExact();
        } catch (ArithmeticException e) {
            LOG.info("DAY-TIME interval literal is outside the native millisecond range: {}", rexNode, e);
            return null;
        }
    }

    private static Map<String, Object> tryBuildTimestampDayTimeArithmetic(RexCall rexCall) {
        if ((rexCall.getKind() != SqlKind.PLUS && rexCall.getKind() != SqlKind.MINUS)
                || rexCall.getOperands().size() != 2
                || !isTimestampType(rexCall)
                || !hasSupportedTimestampPrecision(rexCall)) {
            return null;
        }

        RexNode left = rexCall.getOperands().get(0);
        RexNode right = rexCall.getOperands().get(1);
        RexNode timestamp;
        Long intervalMillis;
        String functionName;
        if (rexCall.getKind() == SqlKind.PLUS && isTimestampType(left)) {
            timestamp = left;
            intervalMillis = getDayTimeIntervalLiteralMillis(right);
            functionName = DATETIME_PLUS_DAY_TIME;
        } else if (rexCall.getKind() == SqlKind.PLUS && isTimestampType(right)) {
            timestamp = right;
            intervalMillis = getDayTimeIntervalLiteralMillis(left);
            functionName = DATETIME_PLUS_DAY_TIME;
        } else if (rexCall.getKind() == SqlKind.MINUS && isTimestampType(left)) {
            timestamp = left;
            intervalMillis = getDayTimeIntervalLiteralMillis(right);
            functionName = DATETIME_MINUS_DAY_TIME;
        } else {
            return null;
        }

        if (intervalMillis == null || !hasSupportedTimestampPrecision(timestamp)) {
            return null;
        }
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("exprType", "FUNCTION");
        jsonMap.put("returnType", RexTypeToIdMap.get("BIGINT"));
        jsonMap.put("function_name", functionName);
        List<Map<String, Object>> arguments = new ArrayList<>();
        arguments.add(buildJsonMap(timestamp));
        arguments.add(buildLongLiteral(intervalMillis));
        jsonMap.put("arguments", arguments);
        return jsonMap;
    }

    /**
     * OmniOperator native function signatures only register OMNI_VARCHAR (15) for string
     * comparisons and casts; no CAST or comparison kernel involves OMNI_CHAR (16). Normalize
     * any CHAR-typed operand in a binary expression subtree to VARCHAR so the native
     * JSONParser resolves to existing {OMNI_VARCHAR, OMNI_VARCHAR} signatures without
     * attempting an unregistered CAST_String_Char.
     * Package-private so the per-category handler classes can reuse it.
     */
    static void normalizeCharToVarchar(Map<String, Object> jsonMap) {
        if (jsonMap == null) {
            return;
        }
        if (RexTypeToIdMap.get("CHAR").equals(jsonMap.get("dataType"))) {
            jsonMap.put("dataType", RexTypeToIdMap.get("VARCHAR"));
        }
        if (RexTypeToIdMap.get("CHAR").equals(jsonMap.get("returnType"))) {
            jsonMap.put("returnType", RexTypeToIdMap.get("VARCHAR"));
        }
    }

    // Sarg endpoint (NlsString for VARCHAR/CHAR, TimestampString for TIMESTAMP) -> primitive,
    // so OmniOperator ParseJSONLiteral reads a plain string/number not a structured object.
    public static Object extractSargEndpoint(Object endpoint) {
        if (endpoint == null) {
            return null;
        }
        if (endpoint instanceof NlsString) {
            return ((NlsString) endpoint).getValue();
        }
        if (endpoint instanceof TimestampString) {
            return ((TimestampString) endpoint).getMillisSinceEpoch();
        }
        if (!(endpoint instanceof Number) && !(endpoint instanceof Boolean) && !(endpoint instanceof String)) {
            LOG.info("extractSargEndpoint unhandled class: {}", endpoint.getClass().getName());
        }
        return endpoint;
    }

    public static Map<String, Object> buildJsonMap(RexNode rexNode) {
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            List<RexNode> operands = rexCall.getOperands();
            // AS(...) is a transparent rename (Table API select() emits AS($i, 'name') for
            // plain column references): evaluate the aliased operand directly and drop the
            // alias literal. Without this the whole Calc is flagged INVALID and vetoes the
            // native replacement of every operator chained with it.
            if (rexCall.getKind() == SqlKind.AS) {
                return buildJsonMap(operands.get(0));
            }
            SqlOperator operator = rexCall.getOperator();
            String operatorName = operator.getName();
            BinaryExprType binaryType = resolveOperatorType(binaryOperatorMap, operatorName);
            UnaryExprType unaryType = resolveOperatorType(unaryOperatorMap, operatorName);
            SpecialExprType udfType = resolveOperatorType(udfOperatorMap, operatorName);
            SpecialExprType specialType = udfType != null ? udfType : resolveOperatorType(specialOperatorMap, operatorName);
            LOG.info("Current rexNode is {}", rexCall.toString());
            Map<String, Object> timestampIntervalExpr = tryBuildTimestampDayTimeArithmetic(rexCall);
            if (timestampIntervalExpr != null) {
                return timestampIntervalExpr;
            }
            Map<String, Object> intervalLiteralExpr = tryBuildIntervalLiteralProjection(rexCall);
            if (intervalLiteralExpr != null) {
                return intervalLiteralExpr;
            } else if (isIntervalType(rexCall)) {
                LOG.info(
                        "Composite or dynamic INTERVAL expression is outside the native literal scope: {}",
                        rexCall);
                return invalidExpression();
            }
            // datetime + interval 在 Calcite 里被折叠成右操作数为 INTERVAL 类型的 PLUS 调用。
            // 它必须在通用 BINARY 分支之前处理：native 无法把 interval 当作普通 BIGINT 加数
            // （年月间隔没有固定毫秒宽度，且 TIME 需要按天回绕），因此下推为专用 FUNCTION。
            if (isDatetimePlus(rexCall)) {
                return handleDatetimePlus(rexCall, operands, jsonMap);
            } else if (rexCall.operands.size() == 2 && binaryType != null) {
                jsonMap.put("exprType",  OperatorExprType.BINARY.name());
                setDataType(rexCall,jsonMap, "returnType");
                jsonMap.put("operator", binaryType.name());
                // 当一侧是 DATE、另一侧是 TIMESTAMP 时，native 的 DATE（epoch 天）必须提升
                // 为 TIMESTAMP（epoch 毫秒）表示，使二元运算在相同单位上比较/相加。
                // 由目标操作数决定是否提升。
                Map<String, Object> leftMap = buildJsonMapWithTemporalPromotion(
                        operands.get(0), operands.get(1));
                normalizeCharToVarchar(leftMap);
                jsonMap.put("left", leftMap);
                Map<String, Object> rightMap = buildJsonMapWithTemporalPromotion(
                        operands.get(1), operands.get(0));
                normalizeCharToVarchar(rightMap);
                jsonMap.put("right", rightMap);
                return jsonMap;
            } else if (rexCall.operands.size() == 1 && unaryType != null) {
                Map<String, Object> childMap = buildJsonMap(operands.get(0));
                if (childMap.containsKey("exprType") && childMap.get("exprType").equals(SpecialExprType.SWITCH)) {
                    return childMap;
                }
                jsonMap.put("exprType",  OperatorExprType.UNARY.name());
                setDataType(rexCall,jsonMap, "returnType");
                jsonMap.put("operator", unaryType.name());
                jsonMap.put("expr", childMap);
                return jsonMap;
            } else if (specialType != null) {
                // Strategy dispatch: replaces the former switch(specialType) block.
                SpecialExprHandler handler = specialHandlerMap.get(specialType);
                if (handler == null) {
                    jsonMap.put("exprType", "INVALID");
                    return jsonMap;
                }
                return handler.handle(rexCall, operands, jsonMap, specialType);
            } else {
                LOG.info("The operator {} is not supported", operator.toString());
                LOG.info("The expression is {} ", rexCall.toString());
                jsonMap.put("operator","INVALID");
                return jsonMap;
            }
        } else if (rexNode instanceof RexLiteral) {
            RexLiteral rexLiteral = (RexLiteral) rexNode;
            if (SqlTypeName.INTERVAL_TYPES.contains(rexLiteral.getType().getSqlTypeName())) {
                return tryBuildIntervalLiteralProjection(rexLiteral);
            }
            jsonMap.put("exprType",  OperatorExprType.LITERAL.name());
            setDataType(rexLiteral, jsonMap, "dataType");
            jsonMap.put("isNull", rexLiteral.isNull());
            if (!rexLiteral.isNull()){
                Object value = rexLiteral.getValue2();
                SqlTypeName typeName = rexLiteral.getType().getSqlTypeName();
                if (typeName == SqlTypeName.DOUBLE && value instanceof Number) {
                    value = ((Number) rexLiteral.getValue()).doubleValue();
                }
                jsonMap.put("value",value);
            }
            // todo: for DECIMAL64D and DECIMAL128, add fields: precision and scale
            return jsonMap;

        } else if (rexNode instanceof RexInputRef){
            RexInputRef inputRef = (RexInputRef) rexNode;
            if (SqlTypeName.INTERVAL_TYPES.contains(inputRef.getType().getSqlTypeName())) {
                return invalidExpression();
            }
            if (inputRef instanceof RexPatternFieldRef || inputRef instanceof RexTableInputRef){
                // we may parse RexTableInputRef later.
                LOG.info("RexPatternFieldRef/RexTableInputRef is not supported.");
                jsonMap.put("exprType",  OperatorExprType.INVALID.name());
                return jsonMap;
            } else { // deal with $index
                RexInputRef  rexInputRef =(RexInputRef) rexNode;
                jsonMap.put("exprType",  OperatorExprType.FIELD_REFERENCE.name());
                setDataType(rexInputRef, jsonMap, "dataType");
                jsonMap.put("colVal", accessIndexMap.getOrDefault(rexInputRef.hashCode(), rexInputRef.hashCode()));
                // todo: add fields precision and scale for DECIMAL64D and DECIMAL128
                return jsonMap;
            }
        } else if (rexNode instanceof RexFieldAccess){
            RexFieldAccess fieldAccess = (RexFieldAccess) rexNode;
            if (SqlTypeName.INTERVAL_TYPES.contains(fieldAccess.getType().getSqlTypeName())) {
                return invalidExpression();
            }
            // Create a field access based on accessIndexMap
            jsonMap.put("exprType",  OperatorExprType.FIELD_REFERENCE.name());
            setDataType(fieldAccess, jsonMap, "dataType");
            int colVal = fieldAccess.getReferenceExpr().hashCode();
            int fieldVal = fieldAccess.getField().getIndex();
            // offset + fieldVal
            jsonMap.put("colVal", accessIndexMap.get(colVal) + fieldVal);
            return jsonMap;
        } else if (rexNode instanceof RexSubQuery) {
            RexSubQuery subQuery = (RexSubQuery) rexNode;
            if (subQuery.getKind() == SqlKind.IN) {
                jsonMap.put("exprType", "IN_SUBQUERY");
                setDataType(subQuery, jsonMap, "returnType");
                // Probe value
                jsonMap.put("value", buildJsonMap(subQuery.operands.get(0)));
                // Subquery result column reference - use subQuery rel hash as column index
                int subqueryColIdx = accessIndexMap.getOrDefault(
                    subQuery.hashCode(), subQuery.hashCode());
                Map<String, Object> subqueryFieldMap = new LinkedHashMap<>();
                subqueryFieldMap.put("exprType", "FIELD_REFERENCE");
                setDataType(subQuery, subqueryFieldMap, "dataType");
                subqueryFieldMap.put("colVal", subqueryColIdx);
                jsonMap.put("subquery_result", subqueryFieldMap);
                jsonMap.put("subquery_col_idx", subqueryColIdx);
                LOG.info("The IN (SUBQUERY) expression is {} ", subQuery.toString());
            } else if (subQuery.getKind() == SqlKind.NOT_IN) {
                jsonMap.put("exprType", "NOT_IN_SUBQUERY");
                setDataType(subQuery, jsonMap, "returnType");
                // Probe value
                jsonMap.put("value", buildJsonMap(subQuery.operands.get(0)));
                // Subquery result column reference - use subQuery rel hash as column index
                int subqueryColIdx = accessIndexMap.getOrDefault(
                    subQuery.hashCode(), subQuery.hashCode());
                Map<String, Object> subqueryFieldMap = new LinkedHashMap<>();
                subqueryFieldMap.put("exprType", "FIELD_REFERENCE");
                setDataType(subQuery, subqueryFieldMap, "dataType");
                subqueryFieldMap.put("colVal", subqueryColIdx);
                jsonMap.put("subquery_result", subqueryFieldMap);
                jsonMap.put("subquery_col_idx", subqueryColIdx);
                LOG.info("The NOT IN (SUBQUERY) expression is {} ", subQuery.toString());
            } else {
                jsonMap.put("exprType", OperatorExprType.INVALID.name());
            }
            return jsonMap;
        } else { // todo: we may consider to parse other types of RexNode later.
            SqlKind kind = rexNode.getKind();
            LOG.info("The RexNode is {}", kind);
            jsonMap.put("exprType",  OperatorExprType.INVALID.name());
            LOG.info("The RexNode is not a RexCall/RexInputRef/RexLiteral. It is not recognized.");
            return jsonMap;
        }
    }

    /**
     * 判定 {@code datetime + interval} 表达式。Calcite 把 SQL 的 interval 加法折叠成
     * 右操作数带 INTERVAL SqlTypeName 的 PLUS 调用。这类调用不能走通用 BINARY 路径
     * （对 native 而言 interval 不是普通数值加数），因此在此识别并路由到
     * {@link #handleDatetimePlus}。
     */
    private static boolean isDatetimePlus(RexCall rexCall) {
        return rexCall.getKind() == SqlKind.PLUS
                && rexCall.getOperands().size() == 2
                && SqlTypeName.INTERVAL_TYPES.contains(
                        rexCall.getOperands().get(1).getType().getSqlTypeName());
    }

    /**
     * 年月间隔（YEAR、YEAR TO MONTH、MONTH）没有固定的毫秒宽度——一个月视日期不同
     * 在 28~31 天之间，因此需要与天时间间隔（DAY/HOUR/MINUTE/SECOND，纯毫秒）不同的
     * native kernel。
     */
    private static boolean isYearMonthInterval(SqlTypeName typeName) {
        return SqlTypeName.YEAR_INTERVAL_TYPES.contains(typeName);
    }

    /**
     * native 的 TIME/TIMESTAMP 是 int64 毫秒，即精度最多为 3。任何精度超过
     * {@link #MAX_NATIVE_TIME_PRECISION} 的 RexNode（微秒/纳秒）在 native 侧会丢数据，
     * 因此直接拒绝并回退到 Flink。非时间类型在此一律放行（其精度与本判定无关）。
     */
    private static boolean hasSupportedNativePrecision(RexNode rexNode) {
        SqlTypeName typeName = rexNode.getType().getSqlTypeName();
        if (typeName != SqlTypeName.TIME && typeName != SqlTypeName.TIMESTAMP) {
            return true;
        }
        int precision = rexNode.getType().getPrecision();
        return precision >= 0 && precision <= MAX_NATIVE_TIME_PRECISION;
    }

    /** 构造 INVALID 标记 JSON；规划器据此判定该 RexNode "不被 native 支持"，
     *  回退到 vanilla Flink 算子执行。 */
    private static Map<String, Object> invalidExpression() {
        Map<String, Object> invalidMap = new LinkedHashMap<>();
        invalidMap.put("exprType", OperatorExprType.INVALID.name());
        return invalidMap;
    }

    /**
     * 将 {@code datetime + interval} RexCall 下推为 native JSON。
     *
     * <p>产出两种形态：
     * <ul>
     *   <li>DATE / TIMESTAMP 操作数 → FUNCTION 节点，调用 {@code datetime_plus_year_month}
     *       或 {@code datetime_plus_day_time}，参数为 (datetime, interval 字面量)。</li>
     *   <li>TIME + year-month → 专用 identity kernel，以区分物理签名相同但日历语义不同的
     *       TIMESTAMP；TIME + day-time → 合成算术树并按天回绕。</li>
     * </ul>
     *
     * <p>当前 interval 仅限字面量：OmniStream 尚未支持 interval 列的编解码，
     * 以错误宽度序列化 interval 列会导致值损坏。
     */
    private static Map<String, Object> handleDatetimePlus(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap) {
        RexNode datetime = operands.get(0);
        RexNode interval = operands.get(1);
        SqlTypeName datetimeType = datetime.getType().getSqlTypeName();
        SqlTypeName returnType = rexCall.getType().getSqlTypeName();
        if (isOutsideDatetimePlusScope(interval, datetime, datetimeType, returnType)) {
            LOG.info("Datetime plus expression is outside the native literal/precision/type scope: {}", rexCall);
            return invalidExpression();
        }

        // DATE 加上不足一天的 interval，其 Rex 返回类型可能是 TIMESTAMP(6)。
        // 但输入只含 epoch 天和整毫秒，因此生成的值是无损的。
        // TIMESTAMP/TIME 输入的精度上限仍维持在 3。
        boolean yearMonth = isYearMonthInterval(interval.getType().getSqlTypeName());
        Map<String, Object> intervalMap = buildIntervalLiteral((RexLiteral) interval, yearMonth);
        // TIME 与 TIMESTAMP 都映射为 BIGINT。年月间隔必须使用独立 identity kernel，
        // 天时间间隔则按 Flink 公式下推为回绕算术树。
        if (datetimeType == SqlTypeName.TIME) {
            return yearMonth
                    ? buildTimePlusYearMonth(datetime, intervalMap)
                    : buildTimePlusDayTime(datetime, intervalMap);
        }

        // DATE / TIMESTAMP：产出 FUNCTION 节点。native kernel 根据 function_name 选择年月
        // 或天时间变体，interval 以带类型字面量的形式传入。
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", yearMonth ? DATETIME_PLUS_YEAR_MONTH : DATETIME_PLUS_DAY_TIME);
        List<Map<String, Object>> arguments = new ArrayList<>();
        arguments.add(buildJsonMap(datetime));
        arguments.add(intervalMap);
        jsonMap.put("arguments", arguments);
        return jsonMap;
    }

    /**
     * 判断 datetime+interval 节点是否超出 native 首版支持范围（字面量/精度/类型）。
     * 每个条件对应一个真实的 native 限制：
     *   - interval 必须是 RexLiteral（暂不支持 interval 列编解码）
     *   - TIMESTAMP_WITH_LOCAL_TIME_ZONE 在 native 无对应表示
     *   - datetime 必须是 DATE/TIME/TIMESTAMP（其它类型与 interval 相加无意义）
     *   - datetime 精度必须 <= 3（native int64-毫秒）
     */
    private static boolean isOutsideDatetimePlusScope(
            RexNode interval, RexNode datetime, SqlTypeName datetimeType, SqlTypeName returnType) {
        return !(interval instanceof RexLiteral)
                || !SqlTypeName.INTERVAL_TYPES.contains(interval.getType().getSqlTypeName())
                || datetimeType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                || returnType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                || (datetimeType != SqlTypeName.DATE
                    && datetimeType != SqlTypeName.TIME
                    && datetimeType != SqlTypeName.TIMESTAMP)
                || !hasSupportedNativePrecision(datetime);
    }

    /**
     * 为 native kernel 序列化 interval 字面量。标量宽度与 interval 类别对应：
     * 年月间隔是总月数（可用 INT，且年月 native kernel 期望 INT）；
     * 天时间间隔是总毫秒数（需 BIGINT，对应天时间 kernel）。
     * {@code getValueAs} 返回的是已聚合好的总值（例如 "2 天 3 小时" → 毫秒数）。
     */
    private static Map<String, Object> buildIntervalLiteral(RexLiteral interval, boolean yearMonth) {
        Map<String, Object> literalMap = new LinkedHashMap<>();
        literalMap.put("exprType", OperatorExprType.LITERAL.name());
        literalMap.put("dataType", yearMonth ? RexTypeToIdMap.get("INT") : RexTypeToIdMap.get("BIGINT"));
        literalMap.put("isNull", interval.isNull());
        if (!interval.isNull()) {
            literalMap.put(
                    "value",
                    yearMonth
                            ? interval.getValueAs(Integer.class)
                            : interval.getValueAs(Long.class));
        }
        return literalMap;
    }

    /** 将 {@code TIME + 年月间隔} 下推为保留 interval NULL 传播的 identity kernel。 */
    private static Map<String, Object> buildTimePlusYearMonth(
            RexNode time, Map<String, Object> intervalMap) {
        Map<String, Object> functionMap = new LinkedHashMap<>();
        functionMap.put("exprType", "FUNCTION");
        functionMap.put("returnType", RexTypeToIdMap.get("BIGINT"));
        functionMap.put("function_name", TIME_PLUS_YEAR_MONTH);
        List<Map<String, Object>> arguments = new ArrayList<>();
        arguments.add(buildJsonMap(time));
        arguments.add(intervalMap);
        functionMap.put("arguments", arguments);
        return functionMap;
    }

    /**
     * 将 {@code TIME + 天时间间隔} 下推为 native 算术。
     *
     * <p>native TIME 是 int64 的"一天内的毫秒数"，取值范围 {@code [0, MILLIS_PER_DAY)}。
     * Flink 对正负 interval 都把结果按天回绕，采用正数取模公式：
     * {@code ((time + MILLIS_PER_DAY + interval % MILLIS_PER_DAY) % MILLIS_PER_DAY)}。
     * {@code + MILLIS_PER_DAY} 一项把可能为负的 {@code time + interval} 抬回非负区间，
     * 使 native 的 MODULUS（遵循 Java/C 向零截断语义，负操作数会得到负余数）仍落在范围内。
     * 先对 interval 按天取模可保持中间值幅度较小。标准的二元 NULL 传播仍然成立：
     * 任一操作数为 NULL 则整棵树为 NULL。
     */
    private static Map<String, Object> buildTimePlusDayTime(
            RexNode time, Map<String, Object> intervalMap) {
        Map<String, Object> day = buildLongLiteral(MILLIS_PER_DAY);
        // interval % MILLIS_PER_DAY —— 真正影响时间部分的"一天内的余数"。
        Map<String, Object> intervalInDay = buildBinaryJson(
                RexTypeToIdMap.get("BIGINT"),
                BinaryExprType.MODULUS,
                intervalMap,
                buildLongLiteral(MILLIS_PER_DAY));
        // time + MILLIS_PER_DAY —— 抬到零以上，保证最终取模结果非负。
        Map<String, Object> positiveTime = buildBinaryJson(
                RexTypeToIdMap.get("BIGINT"),
                BinaryExprType.ADD,
                buildJsonMap(time),
                day);
        // (time + MILLIS_PER_DAY) + intervalInDay —— 未回绕的（可能超过 1 天）总和。
        Map<String, Object> unwrappedResult = buildBinaryJson(
                RexTypeToIdMap.get("BIGINT"),
                BinaryExprType.ADD,
                positiveTime,
                intervalInDay);
        // ... % MILLIS_PER_DAY —— 回绕回 [0, MILLIS_PER_DAY)。
        return buildBinaryJson(
                RexTypeToIdMap.get("BIGINT"),
                BinaryExprType.MODULUS,
                unwrappedResult,
                buildLongLiteral(MILLIS_PER_DAY));
    }

    /** 合成的 BIGINT 字面量，用于下推后的 TIME 算术及 {@link #buildDateToTimestamp}
     *  中的天长常量与零偏移。 */
    private static Map<String, Object> buildLongLiteral(long value) {
        Map<String, Object> literalMap = new LinkedHashMap<>();
        literalMap.put("exprType", OperatorExprType.LITERAL.name());
        literalMap.put("dataType", RexTypeToIdMap.get("BIGINT"));
        literalMap.put("isNull", false);
        literalMap.put("value", value);
        return literalMap;
    }

    /** 组装一个 BINARY 表达式节点 (left op right) 并显式指定返回类型。供下推后的
     *  TIME 算术使用——该算术全程在 BIGINT 上运算，与操作数原类型无关。 */
    private static Map<String, Object> buildBinaryJson(int returnType, BinaryExprType operator,
            Map<String, Object> left, Map<String, Object> right) {
        Map<String, Object> binaryMap = new LinkedHashMap<>();
        binaryMap.put("exprType", OperatorExprType.BINARY.name());
        binaryMap.put("returnType", returnType);
        binaryMap.put("operator", operator.name());
        binaryMap.put("left", left);
        binaryMap.put("right", right);
        return binaryMap;
    }

    /**
     * 判定 {@code value}（DATE）是否需要提升为 TIMESTAMP 以匹配 {@code target} 的类型。
     * native 中 DATE 是 epoch 天、TIMESTAMP 是 epoch 毫秒，若在二元运算或 CASE 分支中
     * 不做提升就混用，会在不兼容的单位上比较/相加，静默产生错误结果。
     */
    private static boolean needsDateToTimestampPromotion(RexNode value, RexNode target) {
        return value.getType().getSqlTypeName() == SqlTypeName.DATE
                && target.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP;
    }

    /**
     * {@link #buildJsonMap} 的包装：当运算上下文（{@code target} RexNode，例如外层二元调用
     * 或 CASE）期望 TIMESTAMP 时，把 DATE 操作数提升为 TIMESTAMP。无需提升的操作数原样透传。
     */
    private static Map<String, Object> buildJsonMapWithTemporalPromotion(
            RexNode value, RexNode target) {
        if (!needsDateToTimestampPromotion(value, target)) {
            return buildJsonMap(value);
        }
        return buildDateToTimestamp(value);
    }

    /**
     * 将 DATE → TIMESTAMP 的提升下推为 {@code datetime_plus_day_time(date, 0)}。
     *
     * <p>DATE 是 epoch 天，而 TIMESTAMP 路径是 epoch 毫秒。加一个零天时间间隔可复用
     * 现有的 DATE32 + 天时间 kernel，把天数换算成毫秒（date_days * MILLIS_PER_DAY），
     * 无需新增 native cast。返回类型为 BIGINT（epoch 毫秒），与 native 的 TIMESTAMP 表示一致。
     */
    private static Map<String, Object> buildDateToTimestamp(RexNode date) {
        Map<String, Object> functionMap = new LinkedHashMap<>();
        functionMap.put("exprType", "FUNCTION");
        functionMap.put("returnType", RexTypeToIdMap.get("BIGINT"));
        functionMap.put("function_name", DATETIME_PLUS_DAY_TIME);
        List<Map<String, Object>> arguments = new ArrayList<>();
        arguments.add(buildJsonMap(date));
        arguments.add(buildLongLiteral(0L));
        functionMap.put("arguments", arguments);
        return functionMap;
    }

    // =========================================================================
    // Core structural handlers kept in this class (wired in specialHandlerMap above).
    // Category-specific handlers live in LogicExprHandlers / JsonExprHandlers /
    // DateTimeExprHandlers / StringExprHandlers / MathExprHandlers. The shared
    // handleSimpleFunction below is package-private so those classes can reference it
    // for FUNCTION-forwarding expressions.
    // =========================================================================

    private static Map<String, Object> handleSwitch(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        int numOperands = operands.size();
        jsonMap.put("exprType",  "SWITCH_GENERAL");
        setDataType(rexCall,jsonMap, "returnType");
        // SWITCH([input_expr, condition1], 'result1', [input_expr, condition2], 'result2', 'optional other')
        int numOfCases = (numOperands - 1) / 2;
        jsonMap.put("numOfCases", numOfCases);

        for (int i = 0; i < numOfCases; i++) {
            Map<String, Object> caseMap = new LinkedHashMap<>();
            caseMap.put("when", buildJsonMap(operands.get(i * 2)));
            RexNode result = operands.get(i * 2 + 1);
            // CASE 表达式会把整体类型提升为所有分支的共同父类型，因此一个 TIMESTAMP 值的
            // CASE 可能存在 DATE 分支。这样的分支必须提升为 TIMESTAMP（epoch 毫秒）以匹配
            // 其它分支的表示，否则 native 会在各分支间混用 epoch 天与 epoch 毫秒。
            caseMap.put("result", buildJsonMapWithTemporalPromotion(result, rexCall));
            caseMap.put("exprType", "WHEN");
            // 被提升的 DATE 分支必须声明 CASE 的 TIMESTAMP 返回类型，而不是自身的 DATE 类型，
            // 使每个分支向 native 声明同一类型。未提升的分支保留自身自然类型。
            if (needsDateToTimestampPromotion(result, rexCall)) {
                setDataType(rexCall, caseMap, "returnType");
            } else {
                setDataType(result, caseMap, "returnType");
            }
            jsonMap.put("Case"+Integer.toString(i + 1), caseMap);
        }
        // else 分支按与上面 WHEN-result 分支相同的规则做提升。
        jsonMap.put(
                "else",
                buildJsonMapWithTemporalPromotion(
                        operands.get(operands.size() - 1), rexCall));
        return jsonMap;
    }

    private static Map<String, Object> handleCoalesce(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        if (operands.isEmpty()) {
            LOG.warn("COALESCE expects at least 1 argument, but got {}", operands.size());
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }

        if (operands.size() == 1) {
            return buildJsonMap(operands.get(0));
        }

        Map<String, Object> nested = null;
        for (int i = operands.size() - 1; i >= 1; i--) {
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("exprType", "COALESCE");
            setDataType(rexCall, node, "returnType");
            node.put("value1", buildJsonMap(operands.get(i - 1)));
            if (nested == null) {
                node.put("value2", buildJsonMap(operands.get(i)));
            } else {
                node.put("value2", nested);
            }
            nested = node;
        }

        if (nested != null) {
            jsonMap.putAll(nested);
        } else {
            jsonMap.put("exprType", "INVALID");
        }
        return jsonMap;
    }

    private static Map<String, Object> handleSearch(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        RexLiteral searchArg = (RexLiteral) operands.get(1);  // Sarg object
        // Get the Sarg value
        Sarg<?> sarg = ((RexLiteral) searchArg).getValueAs(Sarg.class);
        // Check if its a list or a range
        if (sarg.isPoints()) { // A point list - IN expression
            jsonMap.put("exprType", "IN");
            setDataType(rexCall, jsonMap, "returnType");

            List<Map<String, Object>> inArgs = new ArrayList<>();
            inArgs.add(buildJsonMap(operands.get(0)));
            // IN 探针列是 CHAR 时，FIELD_REFERENCE 与字面量都会带 OMNI_CHAR（16），而 native
            // 字符串 IN 只注册了 VARCHAR 签名，必须归一化为 VARCHAR 否则 native 拒绝求值。
            normalizeCharToVarchar(inArgs.get(0));
            // Extract all elements from the list
            List<?> values = new ArrayList<>(sarg.rangeSet.asRanges().stream()
                    .map(range -> range.lowerEndpoint()).collect(Collectors.toList()));

            for (int i = 0; i < values.size(); i++) {
                Map<String, Object> literalMap = new LinkedHashMap<>();
                literalMap.put("exprType", "LITERAL");
                literalMap.put("isNull", false);
                literalMap.put("value", extractSargEndpoint(values.get(i)));
                setDataType(operands.get(0), literalMap, "dataType");
                normalizeCharToVarchar(literalMap);
                inArgs.add(literalMap);
            }
            jsonMap.put("arguments", inArgs);
        } else if (sarg.isComplementedPoints()) {
            // NOT IN: complement point-set Sarg -> recover points -> UNARY(NOT, IN(points)).
            // Guava Range shaded (Flink relocates) -> lambda param type inferred.
            jsonMap.put("exprType", "UNARY");
            setDataType(rexCall, jsonMap, "returnType");
            jsonMap.put("operator", "NOT");
            Map<String, Object> inMap = new LinkedHashMap<>();
            inMap.put("exprType", "IN");
            setDataType(rexCall, inMap, "returnType");
            List<Map<String, Object>> inArgs = new ArrayList<>();
            inArgs.add(buildJsonMap(operands.get(0))); // value to test
            normalizeCharToVarchar(inArgs.get(0));
            sarg.rangeSet.complement().asRanges().forEach(r -> {
                Map<String, Object> literalMap = new LinkedHashMap<>();
                literalMap.put("exprType", "LITERAL");
                literalMap.put("isNull", false);
                literalMap.put("value", extractSargEndpoint(r.lowerEndpoint()));
                setDataType(operands.get(0), literalMap, "dataType");
                normalizeCharToVarchar(literalMap);
                inArgs.add(literalMap);
            });
            inMap.put("arguments", inArgs);
            jsonMap.put("expr", inMap);
        } else { // a range
            // Per-range bounds as [hasLower, hasUpper, lower, upper]; Guava Range not importable (Flink relocates) -> type inferred.
            List<Object[]> rangeInfo = new ArrayList<>();
            sarg.rangeSet.asRanges().forEach(r -> rangeInfo.add(new Object[] {
                r.hasLowerBound(), r.hasUpperBound(),
                r.hasLowerBound() ? r.lowerEndpoint() : null,
                r.hasUpperBound() ? r.upperEndpoint() : null
            }));
            if (rangeInfo.isEmpty()) {
                // Empty Sarg (e.g. SYMMETRIC with an impossible range) evaluates to FALSE
                jsonMap.put("exprType", "LITERAL");
                jsonMap.put("isNull", false);
                jsonMap.put("value", false);
                setDataType(rexCall, jsonMap, "dataType");
            } else {
                Object[] first = rangeInfo.get(0);
                // (-inf..+inf): always TRUE, e.g. reversed-bound NOT BETWEEN.
                boolean alwaysTrue = rangeInfo.size() == 1
                        && !(Boolean) first[0]
                        && !(Boolean) first[1];
                // NOT BETWEEN complement: (-inf..low) U (high..+inf) -> NOT BETWEEN(low, high).
                boolean isComplement = rangeInfo.size() == 2
                        && !(Boolean) first[0]
                        && !(Boolean) rangeInfo.get(1)[1];
                // Normal BETWEEN: single closed range [low..high].
                boolean isClosedRange = rangeInfo.size() == 1
                        && (Boolean) first[0]
                        && (Boolean) first[1];
                if (alwaysTrue) {
                    jsonMap.put("exprType", "LITERAL");
                    jsonMap.put("isNull", false);
                    jsonMap.put("value", true);
                    setDataType(rexCall, jsonMap, "dataType");
                } else if (!isComplement && !isClosedRange) {
                    // Half-bounded or non-complement multi-range Sarg is not representable as BETWEEN.
                    jsonMap.put("exprType", OperatorExprType.INVALID.name());
                } else {
                    // TODO: inclusive, exclusive problem not solved!
                    Object lowerBound = isComplement ? first[3] : first[2];
                    Object upperBound = isComplement ? rangeInfo.get(1)[2] : first[3];

                    Map<String, Object> betweenMap = new LinkedHashMap<>();
                    betweenMap.put("exprType", "BETWEEN");
                    setDataType(rexCall, betweenMap, "returnType");
                    Map<String, Object> valueMap = buildJsonMap(operands.get(0));
                    normalizeCharToVarchar(valueMap);
                    betweenMap.put("value", valueMap);

                    Map<String, Object> lowMap = new LinkedHashMap<>();
                    lowMap.put("exprType", "LITERAL");
                    lowMap.put("isNull", false);
                    lowMap.put("value", extractSargEndpoint(lowerBound));
                    setDataType(operands.get(0), lowMap, "dataType");
                    normalizeCharToVarchar(lowMap);
                    betweenMap.put("lower_bound", lowMap);

                    Map<String, Object> upMap = new LinkedHashMap<>();
                    upMap.put("exprType", "LITERAL");
                    upMap.put("isNull", false);
                    upMap.put("value", extractSargEndpoint(upperBound));
                    setDataType(operands.get(0), upMap, "dataType");
                    normalizeCharToVarchar(upMap);
                    betweenMap.put("upper_bound", upMap);

                    if (isComplement) {
                        // NOT BETWEEN: wrap BETWEEN in UNARY NOT
                        jsonMap.put("exprType", OperatorExprType.UNARY.name());
                        setDataType(rexCall, jsonMap, "returnType");
                        jsonMap.put("operator", UnaryExprType.NOT.name());
                        jsonMap.put("expr", betweenMap);
                    } else {
                        jsonMap.putAll(betweenMap);
                    }
                }
            }
        }
        return jsonMap;
    }

    private static Map<String, Object> handleHashCode(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        Integer returnType = RexTypeToIdMap.get(rexCall.getType().getSqlTypeName().toString());
        if (returnType == 1){
            jsonMap.put("function_name", "mm3hash");
        } else {
            jsonMap.put("function_name", "xxhash64");
        }
        jsonMap.put("function_name", "mm3hash");
        setDataType(rexCall,jsonMap, "returnType");
        List<Object> arguments = new LinkedList<>();
        Map<String, Object> seed = new LinkedHashMap<>();
        // Set the seed to 0
        seed.put("value", 0);
        seed.put("isNull", false);
        seed.put("exprType", "LITERAL");
        seed.put("dataType", returnType);
        arguments.add(buildJsonMap(operands.get(0)));
        arguments.add(seed);
        jsonMap.put("arguments", arguments);
        return jsonMap;
    }

    private static Map<String, Object> handleAnd(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "MULTIPLE_AND_OR");
        jsonMap.put("operator", "AND");
        setDataType(rexCall, jsonMap, "returnType");
        List<Map<String, Object>> cond = new ArrayList<>();
        for (int i=0; i < rexCall.operands.size(); i++) {
            cond.add(buildJsonMap(operands.get(i)));
        }
        jsonMap.put("conditions", cond);
        LOG.info("List is {}", cond.toString());
        LOG.info("The expression is {} ", rexCall.toString());
        return jsonMap;
    }

    private static Map<String, Object> handleOr(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "MULTIPLE_AND_OR");
        jsonMap.put("operator", "OR");
        setDataType(rexCall, jsonMap, "returnType");
        List<Map<String, Object>> cond = new ArrayList<>();
        for (int i=0; i < rexCall.operands.size(); i++) {
            cond.add(buildJsonMap(operands.get(i)));
        }
        jsonMap.put("conditions", cond);
        LOG.info("List is {}", cond.toString());
        LOG.info("The expression is {} ", rexCall.toString());
        return jsonMap;
    }

    private static Map<String, Object> handleCast(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // CAST(date AS TIMESTAMP) 是 DATE→TIMESTAMP 提升（epoch 天 → epoch 毫秒）。
        // 路由到专用提升 kernel，而非通用 cast 路径——通用路径没有 DATE 到 TIMESTAMP 的
        // 单位换算，会把天数当作毫秒处理。
        if (needsDateToTimestampPromotion(operands.get(0), rexCall)) {
            return buildDateToTimestamp(operands.get(0));
        }
        jsonMap.put("exprType", "FUNCTION");
        Map<String, Object> childMap = buildJsonMap(operands.get(0));
        SqlTypeName currentTypeName = rexCall.getType().getSqlTypeName();
        SqlTypeName childTypeName = operands.get(0).getType().getSqlTypeName();
        boolean decimalSameScale = currentTypeName == SqlTypeName.DECIMAL
                && childTypeName == SqlTypeName.DECIMAL
                && rexCall.getType().getScale() == operands.get(0).getType().getScale();
        boolean nonDecimalSameType = currentTypeName != SqlTypeName.DECIMAL
                && childTypeName != SqlTypeName.DECIMAL
                && currentTypeName == childTypeName;
        if (nonDecimalSameType || decimalSameScale) {
            return childMap;
        }
        setDataType(rexCall,jsonMap, "returnType");
        // CAST(VARCHAR/CHAR AS TIMESTAMP): use flink_to_timestamp to parse the string.
        // flink_to_timestamp returns OMNI_LONG (type id 2), not OMNI_TIMESTAMP: OmniStream
        // does not parse microseconds, so DATE/TIMESTAMP cross the operator boundary as
        // OMNI_INT/OMNI_LONG. The returnType must match the operator-side registration,
        // otherwise the signature lookup misses and the whole expression falls back.
        if ((currentTypeName == SqlTypeName.TIMESTAMP)
                && (childTypeName == SqlTypeName.VARCHAR || childTypeName == SqlTypeName.CHAR)) {
            jsonMap.put("returnType", 2);
            jsonMap.put("function_name", "flink_to_timestamp");
            jsonMap.put("expr", childMap);
            List<Map<String, Object>> castArgList2 = new ArrayList<>();
            castArgList2.add(buildJsonMap(operands.get(0)));
            jsonMap.put("arguments", castArgList2);
            return jsonMap;
        }
        jsonMap.put("function_name", specialType.name());
        jsonMap.put("expr", childMap);
        List<Map<String, Object>> castArgList = new ArrayList<>();
        castArgList.add(buildJsonMap(operands.get(0)));
        jsonMap.put("arguments", castArgList);
        return jsonMap;
    }

    private static Map<String, Object> handleDecode(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "decode");
        List<Map<String, Object>> decodeArgList = new ArrayList<>();
        decodeArgList.add(buildJsonMap(operands.get(0)));
        decodeArgList.add(buildJsonMap(operands.get(1)));
        jsonMap.put("arguments", decodeArgList);
        LOG.info("The DECODE expression is {} ", rexCall.toString());
        return jsonMap;
    }

    private static Map<String, Object> handleRegexpReplace(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "regexp_replace");
        List<Map<String, Object>> regexpReplaceArgs = new ArrayList<>();
        // 3 Flink arguments: string, pattern, replacement
        regexpReplaceArgs.add(buildJsonMap(operands.get(0)));
        regexpReplaceArgs.add(buildJsonMap(operands.get(1)));
        regexpReplaceArgs.add(buildJsonMap(operands.get(2)));
        // 4th argument: position = 1 (replace from first character)
        Map<String, Object> positionLiteral = new LinkedHashMap<>();
        positionLiteral.put("exprType", "LITERAL");
        positionLiteral.put("isNull", false);
        positionLiteral.put("value", 1);
        positionLiteral.put("dataType", 1); // INT type
        regexpReplaceArgs.add(positionLiteral);
        jsonMap.put("arguments", regexpReplaceArgs);
        LOG.info("The REGEXP_REPLACE expression is {} ", rexCall.toString());
        return jsonMap;
    }

    private static Map<String, Object> handleTypeOf(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        if (operands.size() < 1 || operands.size() > 2) {
            LOG.warn("TYPEOF expects one input and an optional BOOLEAN literal");
            jsonMap.put("exprType", OperatorExprType.INVALID.name());
            return jsonMap;
        }

        boolean forceSerializable = false;
        if (operands.size() == 2) {
            RexNode forceOperand = operands.get(1);
            if (!(forceOperand instanceof RexLiteral)
                    || forceOperand.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
                LOG.warn("TYPEOF force_serializable must be a BOOLEAN literal");
                jsonMap.put("exprType", OperatorExprType.INVALID.name());
                return jsonMap;
            }
            Boolean forceValue = ((RexLiteral) forceOperand).getValueAs(Boolean.class);
            forceSerializable = Boolean.TRUE.equals(forceValue);
        }

        LogicalType inputType = FlinkTypeFactory.toLogicalType(operands.get(0).getType());
        String typeString;
        if (forceSerializable) {
            try {
                typeString = inputType.asSerializableString();
            } catch (Exception exception) {
                typeString = null;
            }
        } else {
            typeString = inputType.asSummaryString();
        }

        jsonMap.put("exprType", OperatorExprType.LITERAL.name());
        setDataType(rexCall, jsonMap, "dataType");
        jsonMap.put("isNull", typeString == null);
        if (typeString != null) {
            jsonMap.put("value", typeString);
        }
        return jsonMap;
    }

    /**
     * Extract a Calcite FLAG/symbol literal name (e.g. {@code FLAG(BOTH)} -> {@code BOTH}).
     * Package-private so category handlers (JSON wrapping, TRIM flags, FLOOR/CEIL units) can reuse it.
     */
    static String getSymbolLiteralName(RexNode symbolNode) {
        if (symbolNode instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) symbolNode;
            Object literalValue = literal.getValue();
            if (literalValue != null) {
                return normalizeSymbolLiteralName(literalValue.toString());
            }

            Object value2 = literal.getValue2();
            if (value2 != null) {
                return normalizeSymbolLiteralName(value2.toString());
            }
        }

        return normalizeSymbolLiteralName(symbolNode.toString());
    }

    static String normalizeSymbolLiteralName(String symbolText) {
        if (symbolText == null || symbolText.isEmpty()) {
            return null;
        }

        if (symbolText.startsWith("FLAG(") && symbolText.endsWith(")")) {
            symbolText = symbolText.substring(5, symbolText.length() - 1);
        }

        int bracketStart = symbolText.indexOf('[');
        int bracketEnd = symbolText.lastIndexOf(']');
        if (bracketStart >= 0 && bracketEnd > bracketStart) {
            return symbolText.substring(bracketStart + 1, bracketEnd);
        }
        return symbolText;
    }

    private static Map<String, Object> handleEncode(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "encode");
        List<Map<String, Object>> encodeArgList = new ArrayList<>();
        encodeArgList.add(buildJsonMap(operands.get(0)));
        encodeArgList.add(buildJsonMap(operands.get(1)));
        jsonMap.put("arguments", encodeArgList);
        LOG.info("The ENCODE expression is {} ", rexCall.toString());
        return jsonMap;
    }

    /**
     * TIMESTAMP(str): parses a VARCHAR/CHAR string into a TIMESTAMP via flink_to_timestamp.
     * The operator registers it as OMNI_LONG (type id 2, epoch millis) — OmniStream does not
     * parse microseconds, so TIMESTAMP crosses the operator boundary as OMNI_LONG. The
     * returnType must match that registration or the signature lookup misses and the
     * expression silently falls back to non-native evaluation.
     */
    private static Map<String, Object> handleTimestamp(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        jsonMap.put("returnType", 2);
        jsonMap.put("function_name", "flink_to_timestamp");
        Map<String, Object> childMap = buildJsonMap(operands.get(0));
        jsonMap.put("expr", childMap);
        List<Map<String, Object>> argList = new ArrayList<>();
        argList.add(buildJsonMap(operands.get(0)));
        jsonMap.put("arguments", argList);
        return jsonMap;
    }

    private static Map<String, Object> handleTimestampAdd(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        jsonMap.put("returnType", 20); // TIMESTAMP_WITHOUT_TIME_ZONE
        jsonMap.put("function_name", "timestampadd");
        // First operand is the time unit symbol, e.g. FLAG(HOUR)
        String tsAddUnitStr = operands.get(0).toString();
        String tsAddUnitValue = tsAddUnitStr.replaceAll("^FLAG\\((.+)\\)$", "$1");
        Map<String, Object> tsAddUnitLiteral = new LinkedHashMap<>();
        tsAddUnitLiteral.put("exprType", "LITERAL");
        tsAddUnitLiteral.put("dataType", 15); // VARCHAR
        tsAddUnitLiteral.put("isNull", false);
        tsAddUnitLiteral.put("value", tsAddUnitValue);
        List<Map<String, Object>> tsAddArgs = new ArrayList<>();
        tsAddArgs.add(tsAddUnitLiteral);
        for (int i = 1; i < operands.size(); i++) {
            tsAddArgs.add(buildJsonMap(operands.get(i)));
        }
        jsonMap.put("arguments", tsAddArgs);
        LOG.info("The TIMESTAMPADD expression is {} ", rexCall.toString());
        return jsonMap;
    }

    /**
     * Shared handler for expressions that translate to a plain FUNCTION node: the function_name
     * comes from {@link #simpleFunctionNameMap} and every operand is forwarded as an argument,
     * with CHAR literals normalized to VARCHAR. CHAR-typed operands (e.g. a CHAR column feeding
     * a string method) and a CHAR-typed return are normalized to VARCHAR as well, so the emitted
     * signatures match the native VARCHAR registries on both the codegen and the vectorized paths
     * (otherwise mixing a codegen-only CHAR signature with a vectorization-only CHAR signature in
     * one Calc stalls the native evaluator). Any expression whose native signature matches its
     * Flink operands one-to-one can reuse this by registering a name in
     * {@link #simpleFunctionNameMap} plus a handler entry in {@link #specialHandlerMap}.
     * Package-private so the per-category handler classes can reference it.
     */
    static Map<String, Object> handleSimpleFunction(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        normalizeCharToVarchar(jsonMap);
        jsonMap.put("function_name", simpleFunctionNameMap.get(specialType));
        List<Map<String, Object>> simpleArgList = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            Map<String, Object> argMap = buildJsonMap(operands.get(i));
            normalizeCharToVarchar(argMap);
            simpleArgList.add(argMap);
        }
        jsonMap.put("arguments", simpleArgList);
        return jsonMap;
    }

    /**
     * Handle LOG function which has two forms:
     * - LOG(n) with 1 argument: natural logarithm, maps to "ln"
     * - LOG(base, n) with 2 arguments: logarithm with specified base, maps to "log"
     */
    static Map<String, Object> handleLogFunction(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        setDataType(rexCall, jsonMap, "returnType");
        // 1 arg -> "ln", 2 args -> "log"
        String functionName = (operands.size() == 1) ? "ln" : "log";
        jsonMap.put("function_name", functionName);
        List<Map<String, Object>> simpleArgList = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            Map<String, Object> argMap = buildJsonMap(operands.get(i));
            normalizeCharToVarchar(argMap);
            simpleArgList.add(argMap);
        }
        jsonMap.put("arguments", simpleArgList);
        return jsonMap;
    }
}
