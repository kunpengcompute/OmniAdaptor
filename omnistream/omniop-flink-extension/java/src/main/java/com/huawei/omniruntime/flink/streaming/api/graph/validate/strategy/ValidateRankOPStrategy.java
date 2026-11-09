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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;

public class ValidateRankOPStrategy extends AbstractValidateOperatorStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateRankOPStrategy.class);

    // RetractableTopNFunction and FastTop1Function additionally accept DOUBLE and DATE. The
    // native implementations have explicit branches for both, unlike the native JOIN / AGG
    // paths which keep the shared whitelist (no DOUBLE/DATE):
    //   - sort comparison: SortKeyCmp::compareColumn handles OMNI_DATE32 / OMNI_DOUBLE;
    //   - output column copy: AbstractTopNFunction::createOutputBatch handles both;
    //   - retractable equaliser / sort-key projection: rowsEqual / projectSortKey handle both;
    //   - partition-key serialization: rowSerializerCenter/rowDeserializerCenter cover OMNI_DATE32.
    // Verified end to end by RetractableTopNFunctionTest.DateSortKey*. Both functions emit
    // output via AbstractTopNFunction::createOutputBatch. The append-only AppendOnlyTopNFunction
    // (copyColumn: LONG/TIMESTAMP/INT/BOOLEAN/VARCHAR only) supports neither DOUBLE nor DATE
    // output columns, so it keeps the shared whitelist and falls back to vanilla Flink.
    private static final Set<String> RANK_EXTRA_SUPPORT_DATA_TYPE = new HashSet<>(Arrays.asList("DOUBLE", "DATE"));

    private static final List<String> SUPPORT_GROUP_KEY_TYPES = Collections.singletonList("BIGINT");
    private static final List<String> SUPPORT_ORDER_BY_TYPES = Arrays.asList("BIGINT",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)");
    private static final List<String> SUPPORT_FUNCTIONS =
            Arrays.asList("AppendOnlyTopNFunction", "FastTop1Function", "AppendOnlyFirstNFunction", "RetractableTopNFunction");
    private static final Pattern RANK_RANGE_PATTERN = Pattern.compile("rankStart=(\\d+),\\s*rankEnd=(\\d+)");

    // RetractableTopNFunction (general Rank on retractable input): partition keys and sort
    // keys may be any column type the native KeySelector / SortKeyCmp support; multi-column
    // and mixed asc/desc orders are allowed. Type strings are normalized (VARCHAR(n)->VARCHAR).
    private static final List<String> SUPPORT_RETRACTABLE_GROUP_KEY_TYPES = Arrays.asList(
            "CHAR",
            "DATE",
            "BIGINT",
            "INTEGER",
            "VARCHAR",
            "DOUBLE",
            "BOOLEAN",
            "TIME_WITHOUT_TIME_ZONE(0)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(0)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(1)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(2)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)");
    private static final List<String> SUPPORT_RETRACTABLE_ORDER_BY_TYPES = Arrays.asList(
            "BIGINT",
            "INTEGER",
            "DOUBLE",
            "BOOLEAN",
            "VARCHAR",
            "CHAR",
            "DATE",
            "TIME_WITHOUT_TIME_ZONE(0)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(0)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(1)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(2)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)");

    /**
     * Rank-scoped data-type validation: shared whitelist plus the createOutputBatch-function
     * extras (DOUBLE/DATE) when {@code allowRankExtraTypes} is true.
     */
    private boolean validateDataTypes(List<List<String>> dataTypesList, boolean allowRankExtraTypes) {
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

                    if (!SUPPORT_DATA_TYPE.contains(type)
                            && !(allowRankExtraTypes && RANK_EXTRA_SUPPORT_DATA_TYPE.contains(type))) {
                        LOG.info("Unsupported data type: '{}' (original: '{}')", type, originalType);
                        return false;
                    }
                    return true;
                });
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        List<String> inputTypeList = (ArrayList<String>) operatorInfoMap.get("inputTypes");
        List<Integer> partitionKeys = (ArrayList<Integer>) operatorInfoMap.get("partitionKey");

        Object rankFunctionObj = operatorInfoMap.get("processFunction");
        String rankFunction = null;
        if (rankFunctionObj instanceof String) {
            rankFunction = (String) rankFunctionObj;
        } else {
            return false;
        }
        List<Boolean> sortAscendingOrders = (ArrayList<Boolean>) operatorInfoMap.get("sortAscendingOrders");
        List<Integer> sortFieldIds = (ArrayList<Integer>) operatorInfoMap.get("sortFieldIndices");

        // Rank 子算子: FirstN / Top1 / TopN
        if (!SUPPORT_FUNCTIONS.contains(rankFunction)) {
            return false;
        }

        if (CollectionUtil.isNullOrEmpty(partitionKeys) || CollectionUtil.isNullOrEmpty(inputTypeList)) {
            LOG.info("failed inside isNull or empty check");
            return false;
        }

        if ("RetractableTopNFunction".equals(rankFunction)) {
            // General Rank on a retractable (updating/deleting) changelog input.
            if (!validateRetractableTopNFunction(operatorInfoMap, inputTypeList, partitionKeys)) {
                return false;
            }
        } else {
            // AppendOnlyTopNFunction / FastTop1Function (append-only input): strict shape.
            if (partitionKeys.size() > 1) {
                return false;
            }

            for (int partitionKey : partitionKeys) {
                String keyType = stripNotNull(inputTypeList.get(partitionKey));
                if (!SUPPORT_GROUP_KEY_TYPES.contains(keyType)) {
                    LOG.info("failed inside group key type check {} ", keyType);
                    return false;
                }
            }

            if (!validateTop1Function(operatorInfoMap, inputTypeList, sortFieldIds)) {
                return false;
            }

            if (!validateTopNFunction(operatorInfoMap, inputTypeList, sortFieldIds, sortAscendingOrders)) {
                return false;
            }
        }

        if (!validateFirstNFunction(operatorInfoMap, inputTypeList, sortFieldIds, sortAscendingOrders)) {
            return false;
        }

        // if function support,then validate dataTypes
        List<List<String>> dataTypesList = new ArrayList<>(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
        LOG.info("Otherwise failed for dataTypes");
        // DOUBLE/DATE extras are safe only for functions whose output is built by
        // AbstractTopNFunction::createOutputBatch (RetractableTopNFunction, FastTop1Function);
        // AppendOnlyTopNFunction uses copyColumn (LONG/TIMESTAMP/INT/BOOLEAN/VARCHAR only) and
        // would crash on DOUBLE/DATE output columns.
        return validateDataTypes(dataTypesList, !"AppendOnlyTopNFunction".equals(rankFunction));
    }

    /**
     * Validates a general Rank handled by RetractableTopNFunction: partition key and sort key
     * column types must be within the native KeySelector / SortKeyCmp support set.
     */
    private boolean validateRetractableTopNFunction(Map<String, Object> operatorInfoMap, List<String> inputTypeList,
                                                    List<Integer> partitionKeys) {
        for (int partitionKey : partitionKeys) {
            String keyType = normalizeType(inputTypeList.get(partitionKey));
            if (!SUPPORT_RETRACTABLE_GROUP_KEY_TYPES.contains(keyType)) {
                LOG.info("failed inside retractable partition key type check {} ", keyType);
                return false;
            }
        }
        List<Integer> sortFieldIds = (ArrayList<Integer>) operatorInfoMap.get("sortFieldIndices");
        if (CollectionUtil.isNullOrEmpty(sortFieldIds)) {
            // The streaming-Limit degenerate shape (empty sort key) is validated by
            // ValidateLimitOPStrategy; a general Rank always has sort fields. Allow it
            // defensively since the native implementation handles the degenerate case.
            return true;
        }
        for (int sortId : sortFieldIds) {
            String sortType = normalizeType(inputTypeList.get(sortId));
            if (!SUPPORT_RETRACTABLE_ORDER_BY_TYPES.contains(sortType)) {
                LOG.info("failed inside retractable sort key type check {} ", sortType);
                return false;
            }
        }
        return true;
    }

    /** Normalizes a type string for whitelist matching: strips NOT NULL and VARCHAR(n)/CHAR(n). */
    private static String normalizeType(String type) {
        String normalized = stripNotNull(type);
        if (normalized == null) {
            return null;
        }
        if (normalized.matches("^VARCHAR\\([^)]*\\)$")) {
            return "VARCHAR";
        }
        if (normalized.matches("^CHAR\\([^)]*\\)$")) {
            return "CHAR";
        }
        return normalized;
    }

    private boolean validateTop1Function(Map<String, Object> operatorInfoMap, List<String> inputTypeList,
                                         List<Integer> sortFieldIds) {
        // TOP1
        if (!operatorInfoMap.get("processFunction").equals("FastTop1Function")) {
            return true;
        }

        if (sortFieldIds.size() > 2) {
            return false;
        }
        for (int sortId : sortFieldIds) {
            if (!SUPPORT_ORDER_BY_TYPES.contains(stripNotNull(inputTypeList.get(sortId)))) {
                return false;
            }
        }

        return true;
    }

    private boolean validateTopNFunction(Map<String, Object> operatorInfoMap, List<String> inputTypeList,
                                         List<Integer> sortFieldIds, List<Boolean> sortAscendingOrders) {
        // TOP N  more strict condition
        if (!operatorInfoMap.get("processFunction").equals("AppendOnlyTopNFunction")) {
            return true;
        }

        return sortFieldIds.size() <= 1 && stripNotNull(inputTypeList.get(sortFieldIds.get(0))).equals("BIGINT")
                && !sortAscendingOrders.get(0);
    }

    private boolean validateFirstNFunction(Map<String, Object> operatorInfoMap, List<String> inputTypeList,
                                           List<Integer> sortFieldIds, List<Boolean> sortAscendingOrders) {
        if (!operatorInfoMap.get("processFunction").equals("AppendOnlyFirstNFunction")) {
            return true;
        }
        Object rankRangeObj = operatorInfoMap.get("rankRange");
        if (!(rankRangeObj instanceof String)) {
            LOG.info("validate AppendOnlyFirstNFunction missing constant rankRange");
            return false;
        }
        String rankRange = (String) rankRangeObj;
        Matcher matcher = RANK_RANGE_PATTERN.matcher(rankRange);
        if (!matcher.matches()) {
            LOG.info("validate AppendOnlyFirstNFunction only supports constant rankRange, got {}", rankRange);
            return false;
        }
        long rankStart;
        long rankEnd;
        try {
            rankStart = Long.parseLong(matcher.group(1));
            rankEnd = Long.parseLong(matcher.group(2));
        } catch (NumberFormatException exception) {
            LOG.info("validate AppendOnlyFirstNFunction unparsable rankRange {}", rankRange);
            return false;
        }
        if (rankStart < 1 || rankEnd < rankStart || rankEnd == Long.MAX_VALUE) {
            LOG.info("validate AppendOnlyFirstNFunction invalid or unbounded rank range {}", rankRange);
            return false;
        }
        if (CollectionUtil.isNullOrEmpty(sortFieldIds) || CollectionUtil.isNullOrEmpty(sortAscendingOrders)
                || sortFieldIds.size() != 1 || sortAscendingOrders.size() < 1) {
            LOG.info("validate AppendOnlyFirstNFunction requires a single sort field");
            return false;
        }
        if (!Boolean.TRUE.equals(sortAscendingOrders.get(0))) {
            LOG.info("validate AppendOnlyFirstNFunction requires ascending proctime order");
            return false;
        }
        int sortId = sortFieldIds.get(0);
        if (sortId < 0 || sortId >= inputTypeList.size()) {
            LOG.info("validate AppendOnlyFirstNFunction sort index {} out of bound", sortId);
            return false;
        }
        String sortType = stripNotNull(inputTypeList.get(sortId));
        if (sortType == null || !sortType.startsWith("TIMESTAMP_WITH_LOCAL_TIME_ZONE")) {
            LOG.info("validate AppendOnlyFirstNFunction sort type {} is not proctime", sortType);
            return false;
        }
        return true;
    }
}
