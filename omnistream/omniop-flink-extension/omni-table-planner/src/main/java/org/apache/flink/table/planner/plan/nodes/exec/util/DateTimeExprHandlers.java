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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.util.RexNodeUtil.SpecialExprType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Date/time-related special-expression handlers extracted from {@link RexNodeUtil}.
 *
 * <p>Each handler builds the OmniOperator JSON for one date/time function and is wired into
 * {@link RexNodeUtil#specialHandlerMap} by {@link #register()}. Keeping every date/time function
 * in this file means adding/adjusting such an expression only touches this file (plus the shared
 * {@link SpecialExprType} enum).
 */
final class DateTimeExprHandlers {
    private static final Logger LOG = LoggerFactory.getLogger(DateTimeExprHandlers.class);

    private DateTimeExprHandlers() {
    }

    /** Register operator names, native function names and handlers for all date/time expressions. */
    static void register() {
        RexNodeUtil.specialOperatorMap.put("PROCTIME_MATERIALIZE", SpecialExprType.PROCTIME);
        RexNodeUtil.specialOperatorMap.put("PROCTIME", SpecialExprType.PROCTIME);
        RexNodeUtil.specialOperatorMap.put("EXTRACT", SpecialExprType.EXTRACT);
        RexNodeUtil.specialOperatorMap.put("DATE_FORMAT", SpecialExprType.DATE_FORMAT);
        RexNodeUtil.specialOperatorMap.put("TO_TIMESTAMP_LTZ", SpecialExprType.TO_TIMESTAMP_LTZ);
        RexNodeUtil.specialOperatorMap.put("TO_TIMESTAMP", SpecialExprType.TO_TIMESTAMP);
        RexNodeUtil.specialOperatorMap.put("TO_DATE", SpecialExprType.TO_DATE);
        RexNodeUtil.specialOperatorMap.put("CURRENT_TIMESTAMP", SpecialExprType.CURRENT_TIMESTAMP);
        RexNodeUtil.specialOperatorMap.put("NOW", SpecialExprType.CURRENT_TIMESTAMP);
        RexNodeUtil.specialOperatorMap.put("CURRENT_WATERMARK", SpecialExprType.CURRENT_WATERMARK);
        RexNodeUtil.specialOperatorMap.put("DATE_ADD", SpecialExprType.DATE_ADD);
        RexNodeUtil.specialOperatorMap.put("UNIX_TIMESTAMP", SpecialExprType.UNIX_TIMESTAMP);
        RexNodeUtil.specialOperatorMap.put("FROM_UNIXTIME", SpecialExprType.FROM_UNIXTIME);
        RexNodeUtil.specialOperatorMap.put("CONVERT_TZ", SpecialExprType.CONVERT_TZ);
        RexNodeUtil.specialOperatorMap.put("LOCALTIME", SpecialExprType.LOCALTIME);
        RexNodeUtil.specialOperatorMap.put("CURRENT_TIME", SpecialExprType.LOCALTIME);
        RexNodeUtil.specialOperatorMap.put("LOCALTIMESTAMP", SpecialExprType.LOCALTIMESTAMP);
        RexNodeUtil.specialOperatorMap.put("CURRENT_DATE", SpecialExprType.CURRENT_DATE);
        RexNodeUtil.specialOperatorMap.put("CURRENT_ROW_TIMESTAMP", SpecialExprType.CURRENT_ROW_TIMESTAMP);
        RexNodeUtil.specialOperatorMap.put("DATE", SpecialExprType.DATE);
        RexNodeUtil.udfOperatorMap.put("DATE_ADD", SpecialExprType.DATE_ADD);

        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.TO_TIMESTAMP, "flink_to_timestamp");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.CONVERT_TZ, "convert_tz");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.LOCALTIME, "flink_localtime");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.LOCALTIMESTAMP, "flink_localtimestamp");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.CURRENT_DATE, "flink_current_date");
        RexNodeUtil.simpleFunctionNameMap.put(SpecialExprType.CURRENT_ROW_TIMESTAMP, "flink_current_row_timestamp");

        RexNodeUtil.specialHandlerMap.put(SpecialExprType.TO_TIMESTAMP_LTZ, DateTimeExprHandlers::handleToTimestampLtz);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.TO_TIMESTAMP, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.TO_DATE, DateTimeExprHandlers::handleToDate);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.PROCTIME, DateTimeExprHandlers::handleProctime);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.DATE_FORMAT, DateTimeExprHandlers::handleDateFormat);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.CURRENT_TIMESTAMP, DateTimeExprHandlers::handleCurrentTimestamp);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.CURRENT_WATERMARK, DateTimeExprHandlers::handleCurrentWatermark);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.DATE_ADD, DateTimeExprHandlers::handleDateAdd);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.FROM_UNIXTIME, DateTimeExprHandlers::handleFromUnixtime);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.EXTRACT, DateTimeExprHandlers::handleExtract);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.UNIX_TIMESTAMP, DateTimeExprHandlers::handleUnixTimestamp);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.CONVERT_TZ, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.LOCALTIME, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.LOCALTIMESTAMP, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.CURRENT_DATE, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.CURRENT_ROW_TIMESTAMP, RexNodeUtil::handleSimpleFunction);
        RexNodeUtil.specialHandlerMap.put(SpecialExprType.DATE, DateTimeExprHandlers::handleDate);
    }

    static Map<String, Object> handleToTimestampLtz(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "to_timestamp_ltz");
        List<Map<String, Object>> toTimestampLtzArgList = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            toTimestampLtzArgList.add(RexNodeUtil.buildJsonMap(operands.get(i)));
        }
        jsonMap.put("arguments", toTimestampLtzArgList);
        return jsonMap;
    }

    static Map<String, Object> handleToDate(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // TO_DATE(string1[, string2]) -> DATE, default format 'yyyy-MM-dd'.
        // Maps to the vectorized to_date({VARCHAR,VARCHAR}) -> OMNI_INT function
        // (days since epoch carried as INT).
        jsonMap.put("exprType", "FUNCTION");
        // to_date is registered to return OMNI_INT(1); set it explicitly so
        // setDataType does not collapse the DATE return type to LONG(2).
        jsonMap.put("returnType", RexNodeUtil.RexTypeToIdMap.get("INT"));
        jsonMap.put("function_name", "to_date");
        List<Map<String, Object>> toDateArgs = new ArrayList<>();
        Map<String, Object> toDateInputArg = RexNodeUtil.buildJsonMap(operands.get(0));
        RexNodeUtil.normalizeCharToVarchar(toDateInputArg);
        toDateArgs.add(toDateInputArg);
        Map<String, Object> toDateFormatArg;
        if (operands.size() >= 2) {
            toDateFormatArg = RexNodeUtil.buildJsonMap(operands.get(1));
            RexNodeUtil.normalizeCharToVarchar(toDateFormatArg);
        } else {
            // Flink default: TO_DATE(string) -> 'yyyy-MM-dd'
            toDateFormatArg = new LinkedHashMap<>();
            toDateFormatArg.put("exprType", "LITERAL");
            toDateFormatArg.put("dataType", RexNodeUtil.RexTypeToIdMap.get("VARCHAR"));
            toDateFormatArg.put("isNull", false);
            toDateFormatArg.put("value", "yyyy-MM-dd");
            toDateFormatArg.put("width", 10);
        }
        toDateArgs.add(toDateFormatArg);
        jsonMap.put("arguments", toDateArgs);
        LOG.info("The TO_DATE expression is {} ", rexCall.toString());
        return jsonMap;
    }

    /**
     * DATE(string) -> DATE: parses a "yyyy-MM-dd" string into a SQL DATE.
     * Semantically identical to TO_DATE(string, 'yyyy-MM-dd'); reuses the same
     * vectorized to_date({VARCHAR,VARCHAR}) -> OMNI_INT kernel.
     */
    static Map<String, Object> handleDate(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        jsonMap.put("returnType", RexNodeUtil.RexTypeToIdMap.get("INT"));
        jsonMap.put("function_name", "to_date");
        List<Map<String, Object>> dateArgs = new ArrayList<>();
        Map<String, Object> dateInputArg = RexNodeUtil.buildJsonMap(operands.get(0));
        RexNodeUtil.normalizeCharToVarchar(dateInputArg);
        dateArgs.add(dateInputArg);
        // DATE(string) always uses 'yyyy-MM-dd' format
        Map<String, Object> dateFormatArg = new LinkedHashMap<>();
        dateFormatArg.put("exprType", "LITERAL");
        dateFormatArg.put("dataType", RexNodeUtil.RexTypeToIdMap.get("VARCHAR"));
        dateFormatArg.put("isNull", false);
        dateFormatArg.put("value", "yyyy-MM-dd");
        dateFormatArg.put("width", 10);
        dateArgs.add(dateFormatArg);
        jsonMap.put("arguments", dateArgs);
        LOG.info("The DATE expression is {} ", rexCall.toString());
        return jsonMap;
    }

    static Map<String, Object> handleProctime(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        SqlOperator operator = rexCall.getOperator();
        jsonMap.put("exprType", SpecialExprType.PROCTIME);
        jsonMap.put("returnType", RexNodeUtil.RexTypeToIdMap.get(rexCall.getType().getSqlTypeName().toString()));
        LOG.info("The operator is* {} ", operator.getName());
        LOG.info("The type is* {} ", rexCall.getType().getSqlTypeName().toString());
        return jsonMap;
    }

    static Map<String, Object> handleDateFormat(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        Integer returnDataType = RexNodeUtil.RexTypeToIdMap.get(rexCall.getType().getSqlTypeName().toString());
        jsonMap.put("returnType", returnDataType);
        jsonMap.put("width", operands.get(1).getType().getPrecision());
        List<Map<String, Object>> argumentsList = new ArrayList<>();
        Map<String, Object> argMap1 = RexNodeUtil.buildJsonMap(operands.get(0));
        RexNodeUtil.setDataType(operands.get(0), argMap1, "dataType");
        if (!argMap1.getOrDefault("dataType", 2).equals(2)) {
            argMap1.put("value", "INVALID");
        }
        argMap1.put("dataType", 2);
        argumentsList.add(argMap1);
        Map<String, Object> argMap2 = RexNodeUtil.buildJsonMap(operands.get(1));
        argMap2.put("dataType", returnDataType);
        argumentsList.add(argMap2);
        if ("yyyy-MM-dd".equals(argMap2.get("value"))) {
            argMap2.put("value", "%Y-%m-%d");
        } else if ("HH:mm".equals(argMap2.get("value"))) {
            argMap2.put("value", "%H:%M");
        } else if ("HH".equals(argMap2.get("value"))) {
            argMap2.put("value", "%H");
        } else {
            argMap2.put("value", "INVALID");
        }
        if (operands.get(0).getType().getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            jsonMap.put("function_name", "from_unixtime_with_tz");
            Map<String, Object> argMap3 = new LinkedHashMap<>();
            argMap3.put("dataType", 15);
            argMap3.put("exprType", "LITERAL");
            argMap3.put("isNull", false);
            argMap3.put("value", CommonExecCalc.getZoneId().getId());
            argMap3.put("width", CommonExecCalc.getZoneId().getId().length());
            argumentsList.add(argMap3);
        } else {
            jsonMap.put("function_name", "from_unixtime_without_tz");
        }
        jsonMap.put("arguments", argumentsList);
        return jsonMap;
    }

    static Map<String, Object> handleCurrentTimestamp(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "current_timestamp");
        List<Map<String, Object>> currentTimestampArgs = new ArrayList<>();
        if (operands.size() > 0) {
            for (int i = 0; i < operands.size(); i++) {
                currentTimestampArgs.add(RexNodeUtil.buildJsonMap(operands.get(i)));
            }
        }
        jsonMap.put("arguments", currentTimestampArgs);
        LOG.info("The CURRENT_TIMESTAMP expression is {} ", rexCall.toString());
        return jsonMap;
    }

    static Map<String, Object> handleCurrentWatermark(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        if (operands.size() != 1) {
            LOG.warn("CURRENT_WATERMARK expects exactly one rowtime operand, but got {}", operands.size());
            jsonMap.put("exprType", RexNodeUtil.OperatorExprType.INVALID.name());
            return jsonMap;
        }

        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "current_watermark");
        // Flink validates the rowtime operand and derives the logical return type. At runtime,
        // CURRENT_WATERMARK reads operator context and therefore has no data arguments.
        jsonMap.put("arguments", new ArrayList<>());
        return jsonMap;
    }

    static Map<String, Object> handleDateAdd(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        jsonMap.put("returnType", 2);
        jsonMap.put("function_name", "date_add_days");
        List<Map<String, Object>> dateAddArgs = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            Map<String, Object> argMap = RexNodeUtil.buildJsonMap(operands.get(i));
            if (i == 0) {
                SqlTypeName firstArgType = operands.get(0).getType().getSqlTypeName();
                if (firstArgType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                        || SqlTypeName.DATETIME_TYPES.contains(firstArgType)) {
                    argMap.put("dataType", 2);
                }
            }
            dateAddArgs.add(argMap);
        }
        jsonMap.put("arguments", dateAddArgs);
        LOG.info("The DATE_ADD expression is {} ", rexCall.toString());
        return jsonMap;
    }

    static Map<String, Object> handleFromUnixtime(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        RexNodeUtil.setDataType(rexCall, jsonMap, "returnType");
        jsonMap.put("function_name", "from_unixtime");
        List<Map<String, Object>> fromUnixTimeArgs = new ArrayList<>();
        Map<String, Object> fromUnixTimeInputArg = RexNodeUtil.buildJsonMap(operands.get(0));
        RexNodeUtil.setDataType(operands.get(0), fromUnixTimeInputArg, "dataType");
        RexNodeUtil.normalizeCharToVarchar(fromUnixTimeInputArg);
        // from_unixtime is registered as {LONG, VARCHAR}. INTEGER and other numeric
        // inputs are cast to BIGINT first so codegen can match that signature.
        Integer bigintTypeId = RexNodeUtil.RexTypeToIdMap.get("BIGINT");
        if (!bigintTypeId.equals(fromUnixTimeInputArg.get("dataType"))) {
            Map<String, Object> castMap = new LinkedHashMap<>();
            castMap.put("exprType", "FUNCTION");
            castMap.put("function_name", "CAST");
            castMap.put("returnType", bigintTypeId);
            List<Map<String, Object>> castArgs = new ArrayList<>();
            castArgs.add(fromUnixTimeInputArg);
            castMap.put("arguments", castArgs);
            fromUnixTimeInputArg = castMap;
        }
        fromUnixTimeArgs.add(fromUnixTimeInputArg);
        Map<String, Object> fromUnixTimeFormatArg;
        if (operands.size() >= 2) {
            fromUnixTimeFormatArg = RexNodeUtil.buildJsonMap(operands.get(1));
            RexNodeUtil.normalizeCharToVarchar(fromUnixTimeFormatArg);
        } else {
            // Flink default: FROM_UNIXTIME(numeric) -> 'yyyy-MM-dd HH:mm:ss'
            fromUnixTimeFormatArg = new LinkedHashMap<>();
            fromUnixTimeFormatArg.put("exprType", "LITERAL");
            fromUnixTimeFormatArg.put("dataType", RexNodeUtil.RexTypeToIdMap.get("VARCHAR"));
            fromUnixTimeFormatArg.put("isNull", false);
            fromUnixTimeFormatArg.put("value", "yyyy-MM-dd HH:mm:ss");
            fromUnixTimeFormatArg.put("width", 19);
        }
        fromUnixTimeArgs.add(fromUnixTimeFormatArg);
        if (operands.get(0).getType().getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            Map<String, Object> fromUnixTimeTzArg = new LinkedHashMap<>();
            fromUnixTimeTzArg.put("dataType", RexNodeUtil.RexTypeToIdMap.get("VARCHAR"));
            fromUnixTimeTzArg.put("exprType", "LITERAL");
            fromUnixTimeTzArg.put("isNull", false);
            fromUnixTimeTzArg.put("value", CommonExecCalc.getZoneId().getId());
            fromUnixTimeTzArg.put("width", CommonExecCalc.getZoneId().getId().length());
            fromUnixTimeArgs.add(fromUnixTimeTzArg);
        }
        jsonMap.put("arguments", fromUnixTimeArgs);
        return jsonMap;
    }

    static Map<String, Object> handleUnixTimestamp(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        jsonMap.put("exprType", "FUNCTION");
        // UNIX_TIMESTAMP always returns BIGINT -> OMNI_LONG (2).
        jsonMap.put("returnType", RexNodeUtil.RexTypeToIdMap.get("BIGINT"));
        List<Map<String, Object>> args = new LinkedList<>();

        if (operands.isEmpty()) {
            // UNIX_TIMESTAMP() -> current epoch seconds, no tz needed.
            jsonMap.put("function_name", "flink_unix_timestamp");
        } else {
            // String form: always append the session zone-id as the trailing
            // operand (Flink UNIX_TIMESTAMP(string) always applies session tz).
            jsonMap.put("function_name", "flink_unix_timestamp_with_tz");
            // Input string.
            Map<String, Object> inputArg = RexNodeUtil.buildJsonMap(operands.get(0));
            RexNodeUtil.normalizeCharToVarchar(inputArg);
            args.add(inputArg);
            // Optional format (2-arg form).
            if (operands.size() >= 2) {
                Map<String, Object> formatArg = RexNodeUtil.buildJsonMap(operands.get(1));
                RexNodeUtil.normalizeCharToVarchar(formatArg);
                args.add(formatArg);
            }
            // Trailing session zone-id literal (Plan A).
            Map<String, Object> zoneArg = new LinkedHashMap<>();
            zoneArg.put("dataType", 15); // VARCHAR
            zoneArg.put("exprType", "LITERAL");
            zoneArg.put("isNull", false);
            zoneArg.put("value", CommonExecCalc.getZoneId().getId());
            zoneArg.put("width", CommonExecCalc.getZoneId().getId().length());
            args.add(zoneArg);
        }

        jsonMap.put("arguments", args);
        return jsonMap;
    }

    /**
     * Mapping from the EXTRACT flag symbol (the toString of the first operand,
     * e.g. "FLAG(YEAR)") to the native vectorized function name. Covers all 10
     * Flink date/time part extractors. DAYOFMONTH/DAYOFWEEK accept both the
     * standalone-name flag and the underlying TimeUnit flag (DAY / DOW) that
     * Calcite emits for EXTRACT(DAYOFMONTH FROM ...) / EXTRACT(DAYOFWEEK FROM ...).
     */
    private static final java.util.Map<String, String> EXTRACT_FLAG_TO_FUNC;
    static {
        java.util.Map<String, String> m = new java.util.HashMap<>();
        m.put("FLAG(YEAR)", "flink_year");
        m.put("FLAG(QUARTER)", "flink_quarter");
        m.put("FLAG(MONTH)", "flink_month");
        m.put("FLAG(WEEK)", "flink_week");
        m.put("FLAG(DOY)", "flink_dayofyear");
        m.put("FLAG(DAY)", "flink_dayofmonth");
        m.put("FLAG(DOW)", "flink_dayofweek");
        m.put("FLAG(HOUR)", "flink_hour");
        m.put("FLAG(MINUTE)", "flink_minute");
        m.put("FLAG(SECOND)", "flink_second");
        EXTRACT_FLAG_TO_FUNC = java.util.Collections.unmodifiableMap(m);
    }

    static Map<String, Object> handleExtract(RexCall rexCall, List<RexNode> operands,
            Map<String, Object> jsonMap, SpecialExprType specialType) {
        // EXTRACT(<unit> FROM x): operands.get(0) is a RexLiteral symbol whose
        // toString is "FLAG(<UNIT>)" (e.g. "FLAG(YEAR)", "FLAG(HOUR)"). Map it
        // to the matching flink_* vectorized function. For TIMESTAMP_LTZ input
        // append a session zone-id VARCHAR literal and use the flink_*_with_tz
        // variant; otherwise use flink_* (UTC wall-clock, no tz arg).
        String flagStr = operands.get(0).toString();
        String funcName = EXTRACT_FLAG_TO_FUNC.get(flagStr);
        if (funcName == null) {
            jsonMap.put("exprType", "INVALID");
            return jsonMap;
        }

        jsonMap.put("exprType", "FUNCTION");
        // Flink EXTRACT / YEAR / HOUR / MONTH return BIGINT. The vectorized
        // flink_* kernels are registered to write int64 so the sink sees OMNI_LONG.
        jsonMap.put("returnType", RexNodeUtil.RexTypeToIdMap.get("BIGINT"));
        List<Object> args = new LinkedList<>();
        args.add(RexNodeUtil.buildJsonMap(operands.get(1)));
        if (operands.get(1).getType().getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            // Apply the session timezone: use the _with_tz variant and append
            // the zone-id as a VARCHAR literal (same shape the old get_hour_with_tz
            // path produced).
            jsonMap.put("function_name", funcName + "_with_tz");
            Map<String, Object> zoneArg = new LinkedHashMap<>();
            zoneArg.put("dataType", 15);
            zoneArg.put("exprType", "LITERAL");
            zoneArg.put("isNull", false);
            zoneArg.put("value", CommonExecCalc.getZoneId().getId());
            zoneArg.put("width", CommonExecCalc.getZoneId().getId().length());
            args.add(zoneArg);
        } else {
            // Plain TIMESTAMP / DATE / INT: UTC wall-clock, no timezone arg.
            jsonMap.put("function_name", funcName);
        }
        jsonMap.put("arguments", args);
        return jsonMap;
    }
}
