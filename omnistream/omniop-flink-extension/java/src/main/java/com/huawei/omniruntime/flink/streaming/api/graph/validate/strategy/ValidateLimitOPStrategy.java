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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Validate strategy for the STREAMING Limit operator (StreamExecLimit, EXPLAIN
 * name "Limit"). Flink plans a streaming LIMIT as a specialized Rank node: the
 * shadowed StreamExecRank overlay writes the Rank JSON contract with the
 * degenerate shape PartitionSpec.ALL_IN_ONE (empty partitionKey) + SortSpec.ANY
 * (empty sortFieldIndices) + ConstantRankRange(offset+1, offset+fetch) +
 * outputRankNumber=false, and the native side runs the Top-N function family
 * under the shared KeyedProcessOperator id: AppendOnlyTopNFunction (append-only
 * input, LIMIT n) / FastTop1Function (append-only input, LIMIT 1) /
 * RetractableTopNFunction (input containing updates/deletes).
 *
 * <p>This strategy encodes exactly that contract: processFunction must be one of
 * the three natively supported functions, partitionKey and sortFieldIndices must
 * both be EMPTY lists, the rank range must be a constant with a bounded rankEnd
 * (OFFSET without FETCH is rejected by the planner itself), outputRankNumber
 * must be false, and the row types must fit the native output paths (the
 * Top-N output column copy supports BIGINT/INTEGER/BOOLEAN/VARCHAR/TIMESTAMP
 * only - DOUBLE/DECIMAL/TIME/DATE fall back to vanilla Flink).
 */
public class ValidateLimitOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateLimitOPStrategy.class);

    private static final List<String> SUPPORT_FUNCTIONS =
            Arrays.asList("AppendOnlyTopNFunction", "FastTop1Function", "RetractableTopNFunction");

    private static final Pattern RANK_RANGE_PATTERN = Pattern.compile("rankStart=(\\d+),\\s*rankEnd=(\\d+)");

    // Limit-specific type whitelist aligned with the native output column copy:
    // AppendOnlyTopNFunction::copyColumn supports INT/LONG/BOOLEAN/VARCHAR plus
    // the TIMESTAMP variants; everything else falls back to vanilla Flink.
    private static final Set<String> SUPPORT_LIMIT_TYPES = new HashSet<>(Arrays.asList(
            "BIGINT",
            "INTEGER",
            "BOOLEAN",
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

    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        Object processFunction = operatorInfoMap.get("processFunction");
        if (!(processFunction instanceof String) || !SUPPORT_FUNCTIONS.contains(processFunction)) {
            LOG.info("validateStreamLimit not support: processFunction is {}", processFunction);
            return false;
        }
        // ALL_IN_ONE partitioning: the streaming Limit key selector is the empty
        // row, so partitionKey must be present but empty.
        if (!isEmptyList(operatorInfoMap, "partitionKey")) {
            LOG.info("validateStreamLimit not support: partitionKey must be an empty list (ALL_IN_ONE)");
            return false;
        }
        // SortSpec.ANY: no sort fields, arrival order is the rank order.
        if (!isEmptyList(operatorInfoMap, "sortFieldIndices")) {
            LOG.info("validateStreamLimit not support: sortFieldIndices must be an empty list (SortSpec.ANY)");
            return false;
        }
        Object sortAscendingOrders = operatorInfoMap.get("sortAscendingOrders");
        Object sortNullsIsLast = operatorInfoMap.get("sortNullsIsLast");
        if (!(sortAscendingOrders instanceof List) || !((List<?>) sortAscendingOrders).isEmpty()
                || !(sortNullsIsLast instanceof List) || !((List<?>) sortNullsIsLast).isEmpty()) {
            LOG.info("validateStreamLimit not support: sortAscendingOrders/sortNullsIsLast must be empty");
            return false;
        }
        String rankRange = getStringInfo(operatorInfoMap, "rankRange");
        Matcher matcher = rankRange == null ? null : RANK_RANGE_PATTERN.matcher(rankRange);
        if (matcher == null || !matcher.matches()) {
            LOG.info("validateStreamLimit not support: rankRange must look like 'rankStart=1, rankEnd=3'");
            return false;
        }
        long rankStart = Long.parseLong(matcher.group(1));
        long rankEnd = Long.parseLong(matcher.group(2));
        if (rankStart < 1 || rankEnd < rankStart || rankEnd == Long.MAX_VALUE) {
            LOG.info("validateStreamLimit not support: invalid or unbounded rank range {}", rankRange);
            return false;
        }
        Object outputRankNumber = operatorInfoMap.get("outputRankNumber");
        if (!(outputRankNumber instanceof Boolean) || (Boolean) outputRankNumber) {
            LOG.info("validateStreamLimit not support: outputRankNumber must be false");
            return false;
        }
        List<String> inputTypes = getTypeList(operatorInfoMap, "inputTypes");
        List<String> outputTypes = getTypeList(operatorInfoMap, "outputTypes");
        if (inputTypes == null || outputTypes == null) {
            LOG.info("validateStreamLimit not support: inputTypes/outputTypes is missing");
            return false;
        }
        List<List<String>> dataTypesList = new ArrayList<>();
        dataTypesList.add(inputTypes);
        dataTypesList.add(outputTypes);
        return validateLimitDataTypes(dataTypesList);
    }

    /** Returns true only when the field exists and is an EMPTY list. */
    private static boolean isEmptyList(Map<String, Object> operatorInfoMap, String name) {
        Object value = operatorInfoMap.get(name);
        return value instanceof List && ((List<?>) value).isEmpty();
    }

    @SuppressWarnings("unchecked")
    private static List<String> getTypeList(Map<String, Object> operatorInfoMap, String name) {
        Object value = operatorInfoMap.get(name);
        if (!(value instanceof List)) {
            return null;
        }
        try {
            return (List<String>) value;
        } catch (ClassCastException exception) {
            LOG.info("validateStreamLimit not support: {} is not a list of strings", name);
            return null;
        }
    }

    private boolean validateLimitDataTypes(List<List<String>> dataTypesList) {
        for (List<String> dataTypes : dataTypesList) {
            for (String type : dataTypes) {
                String normalized = stripNotNull(type);
                if (normalized.matches("^VARCHAR\\([^)]*\\)$")) {
                    normalized = "VARCHAR";
                }
                if (!SUPPORT_LIMIT_TYPES.contains(normalized)) {
                    LOG.info("validateStreamLimit not support: data type '{}' (original: '{}')", normalized, type);
                    return false;
                }
            }
        }
        return true;
    }
}
