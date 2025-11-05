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
            default:
                return false; // Invalid expr type
        }
    }

    private boolean isSupportedWatermarkStrategy(String strategy) {
        return strategy.equals("BoundedOutOfOrderness") || strategy.equals("MonotonousIncreasing");
    }
}