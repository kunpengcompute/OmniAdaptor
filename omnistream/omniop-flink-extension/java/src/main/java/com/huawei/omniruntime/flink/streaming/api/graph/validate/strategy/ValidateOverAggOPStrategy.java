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
import java.util.Set;

/**
 * Validates the JSON description injected by the StreamExecOverAggregate planner patch.
 *
 * <p>Only descriptions carrying a native "processFunction" marker reach this
 * strategy: "ProcTimeUnboundedPrecedingFunction" (processing-time UNBOUNDED
 * PRECEDING frame) or "ProcTimeRowsBoundedPrecedingFunction" (processing-time
 * ROWS N PRECEDING frame, which must also carry a positive "precedingOffset"
 * window size). Every other OVER shape (rowtime frame, RANGE bounded frame,
 * non-whitelisted aggregates, FILTER / DISTINCT / constant arguments) keeps
 * the plain planner text, fails the JSON parse in OmniGraphOverride and falls
 * back to Java. The strategy re-checks the whitelist as defense in depth and
 * must stay in sync with the C++ operators and the planner-side eligibility
 * check.
 */
public class ValidateOverAggOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateOverAggOPStrategy.class);

    private static final String NATIVE_UNBOUNDED_PROCESS_FUNCTION = "ProcTimeUnboundedPrecedingFunction";

    private static final String NATIVE_ROWS_BOUNDED_PROCESS_FUNCTION =
            "ProcTimeRowsBoundedPrecedingFunction";

    /**
     * SUM0 is listed explicitly: the planner rewrites SUM(x) into
     * "CASE(COUNT(x) > 0, $SUM0(x), NULL)" before the exec node is built, so
     * the aggregate calls in the JSON are COUNT + SUM0. AVG decomposes the
     * same way and is covered by this whitelist too.
     */
    private static final Set<String> SUPPORT_AGG_FUNCTION_NAME = new HashSet<>(Arrays.asList(
            "SUM",
            "SUM0",
            "COUNT",
            "MAX",
            "MIN"));

    /** v1 aggregate-argument types for SUM/MAX/MIN (COUNT accepts any whitelisted type). */
    private static final List<String> SUPPORT_AGG_ARG_TYPES = Collections.singletonList("BIGINT");

    private static final List<String> SUPPORT_PARTITION_KEY_TYPES =
            Arrays.asList("BIGINT", "INTEGER", "VARCHAR(2147483647)");

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        // 1. the marker must be present: descriptions without it are not the
        //    native-compatible shape and must not be validated as such.
        Object processFunction = operatorInfoMap.get("processFunction");
        if (!(processFunction instanceof String)) {
            LOG.info("OverAggregate description without the native processFunction marker: {}",
                    processFunction);
            return false;
        }
        String functionMarker = (String) processFunction;
        boolean rowsBoundedFrame = NATIVE_ROWS_BOUNDED_PROCESS_FUNCTION.equals(functionMarker);
        if (!rowsBoundedFrame && !NATIVE_UNBOUNDED_PROCESS_FUNCTION.equals(functionMarker)) {
            LOG.info("OverAggregate description without the native processFunction marker: {}",
                    processFunction);
            return false;
        }

        // 1b. ROWS-bounded descriptions must carry the window size. The native
        //     operator refuses a non-positive precedingOffset at construction,
        //     so a malformed payload fails validation here and falls back to
        //     Java instead of failing the job at runtime.
        if (rowsBoundedFrame) {
            Object precedingOffset = operatorInfoMap.get("precedingOffset");
            if (!(precedingOffset instanceof Number)
                    || ((Number) precedingOffset).longValue() <= 0) {
                LOG.info("RowsBounded OverAggregate description has no positive precedingOffset: {}",
                        precedingOffset);
                return false;
            }
        }

        Map<String, Object> aggInfoListMap = (Map<String, Object>) operatorInfoMap.get("aggInfoList");
        if (aggInfoListMap == null) {
            LOG.info("OverAggregate description has no aggInfoList.");
            return false;
        }
        List<Map<String, Object>> aggregateCalls =
                (ArrayList<Map<String, Object>>) aggInfoListMap.get("aggregateCalls");
        if (CollectionUtil.isNullOrEmpty(aggregateCalls)) {
            LOG.info("OverAggregate description has no aggregateCalls.");
            return false;
        }

        List<String> inputTypeList = (ArrayList<String>) operatorInfoMap.get("inputTypes");
        if (CollectionUtil.isNullOrEmpty(inputTypeList)) {
            LOG.info("OverAggregate description has no inputTypes.");
            return false;
        }

        // 2. aggregate whitelist
        for (Map<String, Object> aggregateCallMap : aggregateCalls) {
            String name = String.valueOf(aggregateCallMap.get("name"));
            // aggCallName like "SUM($1)", "COUNT(*)"
            int openParen = name.indexOf('(');
            String functionName = openParen > 0 ? name.substring(0, openParen) : name;
            if (functionName.contains("$")) {
                functionName = functionName.replace("$", "");
            }
            if (!SUPPORT_AGG_FUNCTION_NAME.contains(functionName)) {
                LOG.info("OverAggregate does not support aggCall {}", name);
                return false;
            }

            List<Integer> argIndexes = (ArrayList<Integer>) aggregateCallMap.get("argIndexes");
            if (argIndexes != null && !argIndexes.isEmpty()) {
                if (argIndexes.size() > 1) {
                    LOG.info("OverAggregate v1 supports single-argument aggregates only: {}", name);
                    return false;
                }
                // v1 does not support the FILTER clause; the planner encodes
                // "no filter" as -1 in the JSON (see the patch class).
                Object filterArg = aggregateCallMap.get("filterArg");
                if (filterArg instanceof Number && ((Number) filterArg).intValue() >= 0) {
                    LOG.info("OverAggregate v1 does not support FILTER: {}", name);
                    return false;
                }
                if (!"COUNT".equals(functionName)) {
                    int argIndex = argIndexes.get(0);
                    if (argIndex < 0 || argIndex >= inputTypeList.size()) {
                        LOG.info("OverAggregate aggCall arg index {} out of range: {}", argIndex, name);
                        return false;
                    }
                    String argType = stripNotNull(inputTypeList.get(argIndex));
                    if (!SUPPORT_AGG_ARG_TYPES.contains(argType)) {
                        LOG.info("The aggregate data type {} is not supported in aggregate function {}.",
                                argType, functionName);
                        return false;
                    }
                }
            }
        }

        // 3. partition key whitelist (empty grouping = one global partition, always fine)
        List<Integer> grouping = (ArrayList<Integer>) operatorInfoMap.get("grouping");
        if (!CollectionUtil.isNullOrEmpty(grouping)) {
            for (Integer groupKey : grouping) {
                if (groupKey < 0 || groupKey >= inputTypeList.size()) {
                    LOG.info("OverAggregate grouping index {} out of range.", groupKey);
                    return false;
                }
                String keyType = stripNotNull(inputTypeList.get(groupKey));
                if (!SUPPORT_PARTITION_KEY_TYPES.contains(keyType)) {
                    LOG.info("The partition key type {} is not supported.", keyType);
                    return false;
                }
            }
        }

        // 4. generic data type check over input and output rows
        List<List<String>> dataTypesList = new ArrayList<>(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
        return validateDataTypes(dataTypesList);
    }
}
