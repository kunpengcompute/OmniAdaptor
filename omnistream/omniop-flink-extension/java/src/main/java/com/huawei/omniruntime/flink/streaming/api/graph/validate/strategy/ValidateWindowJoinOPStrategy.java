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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ValidateWindowJoinOPStrategy extends AbstractValidateOperatorStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateWindowJoinOPStrategy.class);
    private static final Set<String> SUPPORT_JOIN_TYPE =
            new HashSet<>(Arrays.asList("InnerJoin", "LeftOuterJoin", "RightOuterJoin"));
    private static final Set<String> SUPPORT_TIME_TYPE = new HashSet<>(Collections.singletonList("event"));
    private static final Set<String> SUPPORT_ON_CONDITION_DATA_TYPE = new HashSet<>(Arrays.asList("INTEGER", "INT", "STRING", "BIGINT"));
    private static final Set<String> SUPPORT_NON_EQUI_OPERATOR = new HashSet<>(Arrays.asList(
            "EQUAL",
            "GREATER_THAN_OR_EQUAL",
            "LESS_THAN_OR_EQUAL",
            "GREATER_THAN",
            "LESS_THAN"));

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        String joinType = operatorInfoMap.get("joinType").toString();
        if (!SUPPORT_JOIN_TYPE.contains(joinType)) {
            return false;
        }

        String leftTimeType = getStringInfo(operatorInfoMap, "leftTimeType");
        String rightTimeType = getStringInfo(operatorInfoMap, "rightTimeType");
        if (!SUPPORT_TIME_TYPE.contains(leftTimeType) || !SUPPORT_TIME_TYPE.contains(rightTimeType)) {
            LOG.warn("WindowJoin time types are not supported. leftTimeType = {}, rightTimeType = {}",
                    leftTimeType, rightTimeType);
            return false;
        }

        List<Integer> leftJoinKey = (List<Integer>) operatorInfoMap.get("leftJoinKey");
        List<Integer> rightJoinKey = (List<Integer>) operatorInfoMap.get("rightJoinKey");
        List<String> leftInputTypes = (List<String>) operatorInfoMap.get("leftInputTypes");
        List<String> rightInputTypes = (List<String>) operatorInfoMap.get("rightInputTypes");
        List<String> outputTypes = (List<String>) operatorInfoMap.get("outputTypes");
        Integer leftWindowEndIndex = getInteger(operatorInfoMap.get("leftWindowEndIndex"));
        Integer rightWindowEndIndex = getInteger(operatorInfoMap.get("rightWindowEndIndex"));

        if (leftJoinKey.size() != rightJoinKey.size()) {
            LOG.warn("WindowJoin Key indices do not match");
            return false;
        }

        for (int i = 0; i < leftJoinKey.size(); i++) {
            String leftType = stripNotNull(leftInputTypes.get(leftJoinKey.get(i)));
            String rightType = stripNotNull(rightInputTypes.get(rightJoinKey.get(i)));
            if (!leftType.equals(rightType)) {
                LOG.warn("WindowJoin Key types are not equal. leftType = {}, rightType = {}", leftType, rightType);
                return false;
            }
            if (leftJoinKey.get(i).equals(leftWindowEndIndex) && rightJoinKey.get(i).equals(rightWindowEndIndex)) {
                continue;
            }
            if (!isSupportOnConditionDataType(leftType)) {
                LOG.warn("WindowJoin ON condition data type {} is not supported.", leftType);
                return false;
            }
        }

        Object condition = operatorInfoMap.get("nonEquiCondition");
        if (condition != null && !validateNonEquiCondition((Map<String, Object>) condition, outputTypes.size())) {
            return false;
        }

        return validateDataTypes(getDataTypes(operatorInfoMap, "leftInputTypes", "rightInputTypes", "outputTypes"));
    }

    private boolean validateNonEquiCondition(Map<String, Object> condition, int inputSize) {
        if (!"BINARY".equals(condition.get("exprType"))
                || !RexTypeToIdMap.get("BOOLEAN").equals(condition.get("returnType"))) {
            return false;
        }

        Object operator = condition.get("operator");
        if (!SUPPORT_NON_EQUI_OPERATOR.contains(operator)) {
            LOG.warn("WindowJoin non-equi operator {} is not supported.", operator);
            return false;
        }

        Object left = condition.get("left");
        Object right = condition.get("right");
        if (!validateSimpleOperand(left, inputSize) || !validateSimpleOperand(right, inputSize)) {
            return false;
        }
        return Objects.equals(getOperandDataType(left), getOperandDataType(right));
    }

    @SuppressWarnings("unchecked")
    private boolean validateSimpleOperand(Object operand, int inputSize) {
        if (!(operand instanceof Map)) {
            return false;
        }

        Map<String, Object> operandMap = (Map<String, Object>) operand;
        if (!isSupportNonEquiConditionDataTypeId(operandMap.get("dataType"))) {
            return false;
        }

        if ("FIELD_REFERENCE".equals(operandMap.get("exprType"))) {
            Object colVal = operandMap.get("colVal");
            return colVal instanceof Integer && (int) colVal >= 0 && (int) colVal < inputSize;
        }
        if ("LITERAL".equals(operandMap.get("exprType"))) {
            Object isNull = operandMap.get("isNull");
            return isNull instanceof Boolean && ((boolean) isNull || operandMap.containsKey("value"));
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private Object getOperandDataType(Object operand) {
        return ((Map<String, Object>) operand).get("dataType");
    }

    private boolean isSupportOnConditionDataType(String dataType) {
        if (dataType != null && dataType.matches("^VARCHAR\\([^)]*\\)$")) {
            return true;
        }
        return SUPPORT_ON_CONDITION_DATA_TYPE.contains(dataType);
    }

    private boolean isSupportNonEquiConditionDataTypeId(Object dataTypeId) {
        return RexTypeToIdMap.get("INTEGER").equals(dataTypeId) || RexTypeToIdMap.get("INT").equals(dataTypeId)
                || RexTypeToIdMap.get("BIGINT").equals(dataTypeId) || RexTypeToIdMap.get("VARCHAR").equals(dataTypeId);
    }

    private Integer getInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        }
        return null;
    }
}
