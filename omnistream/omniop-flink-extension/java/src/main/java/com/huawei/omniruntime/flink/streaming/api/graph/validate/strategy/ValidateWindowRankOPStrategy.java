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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validate strategy for the window TVF Top-N operator (StreamExecWindowRank
 * overlay, EXPLAIN name "WindowRank"). The operator runs natively as a
 * SlicingWindowOperator carrying a WindowRankProcessor; the overlay writes the
 * "isWindowRank" JSON contract with the partition key, sort spec, constant rank
 * range, outputRankNumber flag, windowEndIndex and shift time zone.
 *
 * <p>Contract encoded here: isWindowRank must be true, the sort contract arrays
 * must be consistent, the rank range must be a positive constant window
 * (rankStart >= 1, rankEnd >= rankStart), windowEndIndex must point at a
 * TIMESTAMP(3) rowtime column (the native processor reads it as long millis),
 * and the input/output row types must fit the native sort-comparator and
 * output-column-copy whitelist: BIGINT/INTEGER/BOOLEAN/VARCHAR/CHAR/STRING plus
 * DATE (int days), TIME(0-3) (long millis) and the TIMESTAMP(0-3) variants.
 */
public class ValidateWindowRankOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateWindowRankOPStrategy.class);

    // Types the native WindowRankProcessor can sort and copy. TIMESTAMP with
    // precision > 3 cannot carry the millisecond window end and is rejected.
    private static final Set<String> SUPPORT_WINDOW_RANK_TYPES = new HashSet<>(Arrays.asList(
            "BIGINT",
            "INTEGER",
            "BOOLEAN",
            "CHAR",
            "DATE",
            "TIME_WITHOUT_TIME_ZONE(0)",
            "TIME_WITHOUT_TIME_ZONE(1)",
            "TIME_WITHOUT_TIME_ZONE(2)",
            "TIME_WITHOUT_TIME_ZONE(3)",
            "VARCHAR",
            "VARCHAR(2147483647)",
            "VARCHAR(2000)",
            "VARCHAR(9)",
            "STRING",
            "TIMESTAMP_WITHOUT_TIME_ZONE(0)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(1)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(2)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)"));

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        Object isWindowRank = operatorInfoMap.get("isWindowRank");
        if (!(isWindowRank instanceof Boolean) || !(Boolean) isWindowRank) {
            LOG.info("validateWindowRank not support: isWindowRank is {}", isWindowRank);
            return false;
        }

        List<Integer> partitionKey = intList(operatorInfoMap.get("partitionKey"));
        List<Integer> sortFields = intList(operatorInfoMap.get("sortFieldIndices"));
        List<Boolean> ascending = boolList(operatorInfoMap.get("sortAscendingOrders"));
        List<Boolean> nullsIsLast = boolList(operatorInfoMap.get("sortNullsIsLast"));
        if (partitionKey == null || sortFields == null || ascending == null || nullsIsLast == null) {
            LOG.info("validateWindowRank not support: partitionKey/sort contract is missing");
            return false;
        }
        if (sortFields.size() != ascending.size() || sortFields.size() != nullsIsLast.size()) {
            LOG.info("validateWindowRank not support: sort contract arrays have different sizes");
            return false;
        }
        // The sort key must be non-empty: Flink's Window TopN requires an
        // ORDER BY clause (ROW_NUMBER without ORDER BY is rejected upstream).
        if (sortFields.isEmpty()) {
            LOG.info("validateWindowRank not support: sortFieldIndices is empty");
            return false;
        }

        Object rankStartObj = operatorInfoMap.get("rankStart");
        Object rankEndObj = operatorInfoMap.get("rankEnd");
        if (!(rankStartObj instanceof Number) || !(rankEndObj instanceof Number)) {
            LOG.info("validateWindowRank not support: rankStart/rankEnd is missing (non-constant rank range?)");
            return false;
        }
        long rankStart = ((Number) rankStartObj).longValue();
        long rankEnd = ((Number) rankEndObj).longValue();
        if (rankStart < 1 || rankEnd < rankStart) {
            LOG.info("validateWindowRank not support: illegal rank range [{}, {}]", rankStart, rankEnd);
            return false;
        }

        Object windowEndIndexObj = operatorInfoMap.get("windowEndIndex");
        if (!(windowEndIndexObj instanceof Number)) {
            LOG.info("validateWindowRank not support: windowEndIndex is missing");
            return false;
        }

        List<String> inputTypes = (List<String>) operatorInfoMap.get("inputTypes");
        List<String> outputTypes = (List<String>) operatorInfoMap.get("outputTypes");
        if (inputTypes == null || outputTypes == null) {
            LOG.info("validateWindowRank not support: inputTypes/outputTypes is missing");
            return false;
        }
        int windowEndIndex = ((Number) windowEndIndexObj).intValue();
        if (windowEndIndex < 0 || windowEndIndex >= inputTypes.size()) {
            LOG.info("validateWindowRank not support: windowEndIndex {} out of range", windowEndIndex);
            return false;
        }
        String windowEndType = stripNotNull(inputTypes.get(windowEndIndex));
        if (!windowEndType.equals("TIMESTAMP_WITHOUT_TIME_ZONE(3)")
                && !windowEndType.equals("TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)")) {
            LOG.info("validateWindowRank not support: window end column type {}", windowEndType);
            return false;
        }

        List<List<String>> dataTypesList = new ArrayList<>();
        dataTypesList.add(inputTypes);
        dataTypesList.add(outputTypes);
        for (List<String> dataTypes : dataTypesList) {
            for (String type : dataTypes) {
                String normalized = stripNotNull(type);
                if (normalized.matches("^VARCHAR\\([^)]*\\)$")) {
                    normalized = "VARCHAR";
                }
                if (normalized.matches("^CHAR\\([^)]*\\)$")) {
                    normalized = "CHAR";
                }
                if (!SUPPORT_WINDOW_RANK_TYPES.contains(normalized)) {
                    LOG.info(
                            "validateWindowRank not support: data type '{}' (original: '{}')", normalized, type);
                    return false;
                }
            }
        }
        return true;
    }

    private static List<Integer> intList(Object value) {
        if (!(value instanceof List)) {
            return null;
        }
        try {
            return (List<Integer>) value;
        } catch (ClassCastException exception) {
            return null;
        }
    }

    private static List<Boolean> boolList(Object value) {
        if (!(value instanceof List)) {
            return null;
        }
        try {
            return (List<Boolean>) value;
        } catch (ClassCastException exception) {
            return null;
        }
    }
}
