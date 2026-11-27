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
import java.util.Set;

public class ValidateWindowJoinOPStrategy extends AbstractValidateOperatorStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateWindowJoinOPStrategy.class);
    private static final Set<String> SUPPORT_JOIN_TYPE =
            new HashSet<>(Arrays.asList("InnerJoin", "LeftOuterJoin", "RightOuterJoin", "FullOuterJoin"));
    private static final Set<String> SUPPORT_TIME_TYPE = new HashSet<>(Collections.singletonList("event"));

    // The native WindowJoin key selector only carries the BIGINT and string (VARCHAR/STRING)
    // families. Any other equi join-key type falls back to Flink.
    private static final Set<String> SUPPORT_JOIN_KEY_TYPES =
            new HashSet<>(Arrays.asList("BIGINT", "VARCHAR", "STRING"));

    // All outer joins (Left/Right/Full) share the same ordinary-column type policy.
    private static final Set<String> OUTER_JOIN_TYPES =
            new HashSet<>(Arrays.asList("LeftOuterJoin", "RightOuterJoin", "FullOuterJoin"));
    // Outer joins accept DATE in addition to the shared whitelist (native carries it as
    // OMNI_DATE32), but must fall back for types the native copy path cannot handle.
    private static final Set<String> OUTER_JOIN_EXTRA_SUPPORT_DATA_TYPE =
            new HashSet<>(Collections.singletonList("DATE"));
    private static final Set<String> OUTER_JOIN_DENY_DATA_TYPE =
            new HashSet<>(Arrays.asList("DECIMAL64", "DECIMAL128", "DATE64"));
    private static final String TIMESTAMP_WITH_TIME_ZONE_PREFIX = "TIMESTAMP_WITH_TIME_ZONE(";
    private static final String INTERVAL_PREFIX = "INTERVAL";

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

        if (leftJoinKey.size() != rightJoinKey.size()) {
            LOG.warn("WindowJoin Key indices do not match");
            return false;
        }

        // Equi condition: the business join keys must be BIGINT or string (VARCHAR/STRING).
        // Window start/end equalities are stripped from the join spec by the planner and are
        // therefore not part of leftJoinKey/rightJoinKey here.
        for (int i = 0; i < leftJoinKey.size(); i++) {
            String leftRaw = stripNotNull(leftInputTypes.get(leftJoinKey.get(i)));
            String rightRaw = stripNotNull(rightInputTypes.get(rightJoinKey.get(i)));
            String leftType = normalizeJoinKeyType(leftRaw);
            String rightType = normalizeJoinKeyType(rightRaw);

            if (leftType == null || !SUPPORT_JOIN_KEY_TYPES.contains(leftType)) {
                LOG.warn("WindowJoin join key type must be BIGINT or STRING. leftType = {}", leftRaw);
                return false;
            }
            if (!leftType.equals(rightType)) {
                LOG.warn("WindowJoin join key types are not equal. leftType = {}, rightType = {}",
                        leftRaw, rightRaw);
                return false;
            }
        }

        // Non-equi condition: strict BINARY + BOOLEAN, then delegate to the shared Calc validator.
        Object condition = operatorInfoMap.get("nonEquiCondition");
        int totalInputSize = leftInputTypes.size() + rightInputTypes.size();
        if (condition != null) {
            if (!(condition instanceof Map)
                    || !nonEquiConditionCheck((Map<String, Object>) condition, totalInputSize)) {
                return false;
            }
        }

        List<List<String>> dataTypes =
                getDataTypes(operatorInfoMap, "leftInputTypes", "rightInputTypes", "outputTypes");
        if (OUTER_JOIN_TYPES.contains(joinType)) {
            return validateOuterJoinDataTypes(dataTypes, joinType);
        }
        return validateDataTypes(dataTypes);
    }

    /**
     * Outer-join (Left/Right/Full) type policy: allow the shared whitelist plus DATE, but force
     * DECIMAL and TIMESTAMP WITH TIME ZONE to fall back to Flink because the native copy path
     * cannot carry them. INTERVAL and container types are absent from the shared whitelist and
     * therefore fall back as well. Everything outside this strict set
     * (DOUBLE/SMALLINT/TINYINT/FLOAT/BINARY/...) is also rejected so the vertex falls back to
     * Flink. All outer join types share this policy so their ordinary-column support stays
     * consistent.
     */
    private boolean validateOuterJoinDataTypes(List<List<String>> dataTypesList, String joinType) {
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

                    if (OUTER_JOIN_DENY_DATA_TYPE.contains(type)
                            || type.startsWith(TIMESTAMP_WITH_TIME_ZONE_PREFIX)
                            || type.startsWith(INTERVAL_PREFIX)) {
                        LOG.info("WindowJoin {} unsupported data type: '{}' (original: '{}')",
                                joinType, type, originalType);
                        return false;
                    }

                    boolean supported =
                            SUPPORT_DATA_TYPE.contains(type) || OUTER_JOIN_EXTRA_SUPPORT_DATA_TYPE.contains(type);
                    if (!supported) {
                        LOG.info("WindowJoin {} unsupported data type: '{}' (original: '{}')",
                                joinType, type, originalType);
                        return false;
                    }
                    return true;
                });
    }

    /**
     * Strict non-equi validation for WindowJoin. The top-level condition must be a binary
     * expression whose return type is BOOLEAN; everything else (UNARY/MULTIPLE_AND_OR/FUNCTION/
     * IN/BETWEEN/...) falls back to Flink. The expression shape and operands are then validated by
     * the shared Calc validator so the native JIT never receives an unvalidated condition.
     */
    private static boolean nonEquiConditionCheck(Map<String, Object> condition, int inputSize) {
        if (condition == null || !"BINARY".equals(condition.get("exprType"))) {
            LOG.warn("WindowJoin non-equi condition must be a binary expression. exprType = {}",
                    condition == null ? null : condition.get("exprType"));
            return false;
        }
        if (!RexTypeToIdMap.get("BOOLEAN").equals(condition.get("returnType"))) {
            LOG.warn("WindowJoin non-equi condition return type must be BOOLEAN.");
            return false;
        }
        return ValidateCalcOPStrategy.validateCalcExpr(condition, inputSize);
    }

    /**
     * Normalizes an equi join-key type to the native key family. VARCHAR(n) and STRING are both
     * carried as OMNI_VARCHAR. Any other type (INTEGER/DATE/BOOLEAN/CHAR/TIME/TIMESTAMP/...)
     * returns null so the vertex falls back to Flink.
     */
    private String normalizeJoinKeyType(String dataType) {
        String type = stripNotNull(dataType);
        if (type == null) {
            return null;
        }
        if (type.matches("^VARCHAR\\([^)]*\\)$") || "STRING".equals(type)) {
            return "VARCHAR";
        }
        if ("BIGINT".equals(type)) {
            return "BIGINT";
        }
        return null;
    }
}
