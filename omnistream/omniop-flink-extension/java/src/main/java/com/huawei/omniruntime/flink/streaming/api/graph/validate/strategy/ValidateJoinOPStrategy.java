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

import static org.apache.flink.util.Preconditions.checkState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ValidateJoinOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateJoinOPStrategy.class);
    // LeftSemiJoin (EXISTS) / LeftAntiJoin (NOT EXISTS) share StreamingSemiAntiJoinOperator.
    private static final Set<String> SUPPORT_JOIN_TYPE =
        new HashSet<>(Arrays.asList("InnerJoin", "LeftOuterJoin", "RightOuterJoin", "FullOuterJoin", "LeftSemiJoin", "LeftAntiJoin"));
    private static final List<String> SUPPORT_JOIN_KEY_TYPES = Collections.singletonList("BIGINT");

    private static boolean nonEquiConditionCheck(Map<String, Object> condition, int inputSize) {
        checkState(condition.get("exprType") instanceof String);
        String exprType = (String) condition.get("exprType");
        if (exprType.equals("BINARY")) {
            if (!condition.get("returnType").equals(RexTypeToIdMap.get("BOOLEAN"))) {
                LOG.warn("Expression type is binary, but return type is not a boolean");
                return false;
            }
            return ValidateCalcOPStrategy.validateCalcExpr(condition, inputSize);
        }

        if (exprType.equals("INVALID") || exprType == null) {
            return false;
        }
        if (exprType.equals("FIELD_ACCESS")) {
            return false;
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        // Checks if join type is valid
        String joinType = operatorInfoMap.get("joinType").toString();
        if (!SUPPORT_JOIN_TYPE.contains(joinType)) {
            return false;
        }
        List<Integer> leftJoinKey = (List<Integer>) operatorInfoMap.get("leftJoinKey");
        List<Integer> rightJoinKey = (List<Integer>) operatorInfoMap.get("rightJoinKey");
        List<String> leftInputTypes = (List<String>) operatorInfoMap.get("leftInputTypes");
        List<String> rightInputTypes = (List<String>) operatorInfoMap.get("rightInputTypes");
        List<String> outputTypes = (List<String>) operatorInfoMap.get("outputTypes");

        // Checks if leftJoinKey and rightJoinKey sizes are the same
        if (leftJoinKey.size() != rightJoinKey.size() || leftJoinKey.size() == 0) {
            LOG.warn("Join Key indices do not match or key size is 0");
            return false;
        }

        // Checks if key types are equal and must be BIGINT
        for (int i = 0; i < leftJoinKey.size(); i++) {
            String leftType = stripNotNull(leftInputTypes.get(leftJoinKey.get(i)));
            String rightType = stripNotNull(rightInputTypes.get(rightJoinKey.get(i)));
            if (!SUPPORT_JOIN_KEY_TYPES.contains(leftType)) {
                LOG.warn("Join Key type must be BIGINT. leftType = {} (original: {})", leftType, leftInputTypes.get(leftJoinKey.get(i)));
                return false;
            }
            if (!leftType.equals(rightType)) {
                LOG.warn("Join Key types are not equal. leftType = {}, rightType = {}",
                        leftInputTypes.get(leftJoinKey.get(i)), rightInputTypes.get(rightJoinKey.get(i)));
                return false;
            }
        }

        // Checks if inputSpec is allowed.
        // The native join state view (InputSideHasNoUniqueKey) stores records as a
        // per-(joinKey, recordHash) count map, which is a superset of the
        // JoinKeyContainsUniqueKey semantics (each key maps to exactly one record,
        // i.e. count is always 1). Therefore JoinKeyContainsUniqueKey inputs --
        // e.g. the deduplicated subquery side produced by IN (subquery) rewrites --
        // are handled correctly by the generic NoUniqueKey path.
        // InputSideHasUniqueKey inputs are also covered: update/retract messages carry the
        // full old row, so locating state entries by row content is equivalent to locating
        // them by unique key, and the per-row count stays 1 for unique-key inputs.
        if (!operatorInfoMap.get("leftInputSpec").equals("NoUniqueKey")
                && !operatorInfoMap.get("leftInputSpec").equals("JoinKeyContainsUniqueKey")
                && !operatorInfoMap.get("leftInputSpec").equals("InputSideHasUniqueKey")) {
            LOG.warn("leftInputSpec only supports NoUniqueKey/JoinKeyContainsUniqueKey/InputSideHasUniqueKey");
            return false;
        }

        if (!operatorInfoMap.get("rightInputSpec").equals("NoUniqueKey")
                && !operatorInfoMap.get("rightInputSpec").equals("JoinKeyContainsUniqueKey")
                && !operatorInfoMap.get("rightInputSpec").equals("InputSideHasUniqueKey")) {
            LOG.warn("rightInputSpec only supports NoUniqueKey/JoinKeyContainsUniqueKey/InputSideHasUniqueKey");
            return false;
        }

        // Checks if condition is valid
        Object condition = operatorInfoMap.get("nonEquiCondition");
        if (condition != null) {
            // v1: semi/anti join supports equi-key only (numAssociate tracking assumes key-only match);
            // non-equi semi/anti falls back to vanilla.
            if (joinType.equals("LeftSemiJoin") || joinType.equals("LeftAntiJoin")) {
                LOG.warn("Semi/anti join supports equi-key only in v1, fall back for non-equi condition");
                return false;
            }
            // RIGHT/FULL outer joins handle non-equi conditions the same way as Flink:
            // the matcher (of()) applies the non-equi filter per candidate BEFORE any
            // numAssociate counting, so key-equal pairs rejected by the predicate do not
            // increment the count and the outer row keeps its null padding. Verified by
            // StreamJoinOuter_sql_test.sql cases 6/7 (dual-mode normalized comparison).
            if (!nonEquiConditionCheck((Map<String, Object>) condition, outputTypes.size())) {
                return false;
            }
        }

        // Checks if data types are valid
        return validateDataTypes(getDataTypes(operatorInfoMap, "leftInputTypes", "rightInputTypes", "outputTypes"));
    }
}
