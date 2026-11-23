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

package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ValidateWatermarkOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateWatermarkOPStrategy.class);

    private static final Set<String> SUPPORT_BINARYOP_NAME = new HashSet<>(Arrays.asList(
            "ADD",
            "SUBTRACT",
            "MULTIPLY",
            "DIVIDE",
            "MODULUS"));
    private static final Set<String> SUPPORT_UNARYOP_NAME = new HashSet<>(Arrays.asList("CAST", "NEGATION"));
    private static final String DATETIME_PLUS_DAY_TIME = "datetime_plus_day_time";
    private static final String DATETIME_MINUS_DAY_TIME = "datetime_minus_day_time";

    // WatermarkAssigner 只消费 rowtime 列，其余列整体透传；DATE 在 native 侧由 OMNI_DATE32 承载，
    // 但共享白名单未包含 DATE。仅在本算子内放行，避免放宽其它算子的支持范围。
    private static final Set<String> EXTRA_SUPPORT_DATA_TYPE = new HashSet<>(Arrays.asList("DATE"));

    @Override
    public boolean validateDataTypes(List<List<String>> dataTypesList) {
        return dataTypesList.stream()
                .flatMap(List::stream)
                .allMatch(type -> {
                    String originalType = type;
                    type = stripNotNull(type);

                    if (type.matches("^VARCHAR\\([^)]*\\)$")) {
                        type = "VARCHAR";
                    }

                    if (type.matches("^DECIMAL64\\([^)]*\\)$")) {
                        type = "DECIMAL64";
                    }

                    if (type.matches("^DECIMAL128\\([^)]*\\)$")) {
                        type = "DECIMAL128";
                    }

                    if (!SUPPORT_DATA_TYPE.contains(type) && !EXTRA_SUPPORT_DATA_TYPE.contains(type)) {
                        LOG.info("Unsupported data type: '{}' (original: '{}')", type, originalType);
                        return false;
                    }
                    return true;
                });
    }

    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        int inputSize = 0;
        if (operatorInfoMap.containsKey("inputTypes")) {
            List<String> inputTypes = (List<String>) operatorInfoMap.get("inputTypes");
            inputSize = inputTypes.size();
            if (CollectionUtil.isNullOrEmpty(inputTypes)) {
                return false;
            }
        } else {
            LOG.info("Missing inputTypes field.");
            return false;
        }
        if (operatorInfoMap.containsKey("outputTypes")) {
            List<String> outputTypes = (List<String>) operatorInfoMap.get("outputTypes");
            if (CollectionUtil.isNullOrEmpty(outputTypes)) {
                return false;
            }
        } else {
            LOG.info("Missing outputTypes field.");
            return false;
        }

        // check condition
        if (operatorInfoMap.containsKey("config")) {
            Map<String, Object> watermarkExpr = (Map<String, Object>) operatorInfoMap.get("config");
            if ((watermarkExpr != null) && !validateCalcExpr(watermarkExpr, inputSize)) {
                return false;
            }
        }


        Long idleTimeout = Long.parseLong(operatorInfoMap.getOrDefault("idleTimeout", 0L).toString());

        // Validate allowed lateness (should be non-negative)
        if (idleTimeout < 0) {
            LOG.warn("idle timeout must be non-negative, but got: {}", idleTimeout);
            return false;
        }


        if (operatorInfoMap.containsKey("rowtimeFieldIndex")) {
            int rowtimeFieldIndex = (int) operatorInfoMap.get("rowtimeFieldIndex");
            if (rowtimeFieldIndex >= inputSize) {
                return false;
            }
        } else {
            LOG.warn("RowTimeFieldIndex do not exist.");
            return false;
        }
        // check dataTypes
        return validateDataTypes(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));

    }

    private boolean validateCalcExpr(Map<String, Object> exprMap, int inputSize) {
        if (!exprMap.containsKey("exprType")) {
            return false; // If any map doesn't contain "expr", return false
        }
        String exprType = (String) exprMap.get("exprType");
        switch (exprType) {
            case "BINARY":
                String binaryOperatorType = (String) exprMap.get("operator");
                if (!SUPPORT_BINARYOP_NAME.contains(binaryOperatorType)) {
                    return false;
                }
                if (!exprMap.containsKey("returnType") || !exprMap.containsKey("left") || !exprMap.containsKey("right")) {
                    return false;
                }
                Object leftExpr = exprMap.get("left");
                if (leftExpr instanceof Map) {
                    // recursive call validateCalcExpr
                    if (!validateCalcExpr((Map<String, Object>) leftExpr, inputSize)) {
                        return false;
                    }
                } else {
                    LOG.error("Cannot parse the left expr in a binary expression: {}", binaryOperatorType);
                    return false;
                }
                Object rightExpr = exprMap.get("right");
                if (rightExpr instanceof Map) {
                    // recursive call validateCalcExpr
                    if (!validateCalcExpr((Map<String, Object>) rightExpr, inputSize)) {
                        return false;
                    }
                } else {
                    LOG.error(" Cannot parse the right expr in a binary expression: {}", binaryOperatorType);
                    return false;
                }
                return true;
            case "UNARY":
                String unaryOperatorType = (String) exprMap.get("operator");
                if (!SUPPORT_UNARYOP_NAME.contains(unaryOperatorType)) {
                    return false;
                }
                if (!exprMap.containsKey("returnType") || !exprMap.containsKey("expr")) {
                    return false;
                }
                Object expr = exprMap.get("expr");
                if (expr instanceof Map) {
                    // recursive call validateCalcExpr
                    if (!validateCalcExpr((Map<String, Object>) expr, inputSize)) {
                        return false;
                    }
                } else {
                    LOG.error("Cannot parse expr in an unary expression: {}", unaryOperatorType);
                    return false;
                }
                // we currently only deal with a fake CAST

                LOG.info("WARNING: CAST/NEGATION might not be supported.");
                return true;
            case "LITERAL":
                if (!exprMap.containsKey("dataType") || !exprMap.containsKey("isNull")) {
                    return false;
                }
                if (!(boolean) exprMap.get("isNull") && !exprMap.containsKey("value")) {
                    return false;
                }
                return true;
            case "FIELD_REFERENCE":
                if (!exprMap.containsKey("dataType") || !exprMap.containsKey("colVal")) {
                    return false;
                }
                Object colVal = exprMap.get("colVal");
                if (colVal instanceof Integer) {
                    if ((int) colVal < 0 || (int) colVal >= inputSize) {
                        LOG.info("Column value in a FIELD_REFERENCE is out of bound");
                        return false;
                    }
                } else {
                    LOG.info("Cannot parse column value in a FIELD_REFERENCE");
                    return false;
                }
                return true;
            case "FUNCTION":
                return validateDatetimeIntervalFunction(exprMap, inputSize);
            default:
                LOG.info("Unsupported watermark exprType: {}", exprType);
                return false; // Invalid expr type
        }
    }

    /**
     * INTERVAL watermark expressions are lowered to {@code datetime_plus_day_time} /
     * {@code datetime_minus_day_time}. Native WatermarkAssigner only consumes
     * {@code rowtimeFieldIndex} and {@code intervalSecond}; this check keeps the JSON
     * config aligned with that rewrite so the operator is not rejected as NOT SUITABLE.
     */
    @SuppressWarnings("unchecked")
    private boolean validateDatetimeIntervalFunction(Map<String, Object> exprMap, int inputSize) {
        if (!exprMap.containsKey("returnType")
                || !exprMap.containsKey("function_name")
                || !exprMap.containsKey("arguments")) {
            return false;
        }
        Object functionNameObj = exprMap.get("function_name");
        if (!(functionNameObj instanceof String)) {
            LOG.info("Watermark function_name is not a string");
            return false;
        }
        String functionName = (String) functionNameObj;
        if (!DATETIME_PLUS_DAY_TIME.equals(functionName)
                && !DATETIME_MINUS_DAY_TIME.equals(functionName)) {
            LOG.info("Watermark does not support function {}", functionName);
            return false;
        }
        Object argumentsObj = exprMap.get("arguments");
        if (!(argumentsObj instanceof List)) {
            LOG.info("Watermark {} arguments is not a list", functionName);
            return false;
        }
        List<?> args = (List<?>) argumentsObj;
        if (args.size() != 2) {
            LOG.info("Watermark {} expects 2 arguments, but got {}", functionName, args.size());
            return false;
        }
        for (Object arg : args) {
            if (!(arg instanceof Map) || !validateCalcExpr((Map<String, Object>) arg, inputSize)) {
                LOG.info("Invalid argument in watermark function {}", functionName);
                return false;
            }
        }
        return true;
    }

    private boolean isSupportedWatermarkStrategy(String strategy) {
        return strategy.equals("BoundedOutOfOrderness") || strategy.equals("MonotonousIncreasing");
    }
}
