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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the STREAMING Limit {@link ValidateLimitOPStrategy}. */
class ValidateLimitOPStrategyTest {

    private final ValidateLimitOPStrategy strategy = new ValidateLimitOPStrategy();

    private static Map<String, Object> validDescription() {
        // Mirrors the JSON written by the shadowed StreamExecRank overlay for a
        // StreamExecLimit node (LIMIT 3): ALL_IN_ONE + SortSpec.ANY.
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("originDescription", "Limit[12]");
        jsonMap.put("inputTypes", Arrays.asList("BIGINT", "BIGINT"));
        jsonMap.put("outputTypes", Arrays.asList("BIGINT", "BIGINT"));
        jsonMap.put("partitionKey", Collections.emptyList());
        jsonMap.put("outputRankNumber", Boolean.FALSE);
        jsonMap.put("rankRange", "rankStart=1, rankEnd=3");
        jsonMap.put("generateUpdateBefore", Boolean.FALSE);
        jsonMap.put("processFunction", "AppendOnlyTopNFunction");
        jsonMap.put("sortFieldIndices", Collections.emptyList());
        jsonMap.put("sortAscendingOrders", Collections.emptyList());
        jsonMap.put("sortNullsIsLast", Collections.emptyList());
        return jsonMap;
    }

    @Test
    @DisplayName("valid streaming LIMIT 3 description passes validation")
    void testValidLimitN() {
        assertTrue(strategy.executeValidateOperator(validDescription()));
    }

    @Test
    @DisplayName("valid streaming LIMIT 1 (FastTop1Function) passes validation")
    void testValidLimit1() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("rankRange", "rankStart=1, rankEnd=1");
        jsonMap.put("processFunction", "FastTop1Function");
        assertTrue(strategy.executeValidateOperator(jsonMap));
    }

    @Test
    @DisplayName("valid streaming LIMIT on updating input (RetractableTopNFunction) passes validation")
    void testValidRetractableLimit() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("processFunction", "RetractableTopNFunction");
        assertTrue(strategy.executeValidateOperator(jsonMap));

        Map<String, Object> offset = validDescription();
        offset.put("processFunction", "RetractableTopNFunction");
        offset.put("rankRange", "rankStart=3, rankEnd=4");
        offset.put("generateUpdateBefore", Boolean.TRUE);
        assertTrue(strategy.executeValidateOperator(offset));
    }

    @Test
    @DisplayName("valid streaming LIMIT n OFFSET m passes validation")
    void testValidOffsetLimit() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("rankRange", "rankStart=3, rankEnd=4");
        assertTrue(strategy.executeValidateOperator(jsonMap));
    }

    @Test
    @DisplayName("unsupported process functions are rejected (unknown function falls back)")
    void testUnsupportedProcessFunction() {
        for (String function : Arrays.asList("UpdatableTopNFunction", "AppendOnlyFirstNFunction", null)) {
            Map<String, Object> jsonMap = validDescription();
            if (function == null) {
                jsonMap.remove("processFunction");
            } else {
                jsonMap.put("processFunction", function);
            }
            assertFalse(strategy.executeValidateOperator(jsonMap), "function: " + function);
        }
    }

    @Test
    @DisplayName("non-empty partitionKey or sortFieldIndices is rejected (not the Limit shape)")
    void testNonEmptyKeysRejected() {
        Map<String, Object> partition = validDescription();
        partition.put("partitionKey", Arrays.asList(0));
        assertFalse(strategy.executeValidateOperator(partition));

        Map<String, Object> sort = validDescription();
        sort.put("sortFieldIndices", Arrays.asList(1));
        assertFalse(strategy.executeValidateOperator(sort));

        Map<String, Object> missingPartition = validDescription();
        missingPartition.remove("partitionKey");
        assertFalse(strategy.executeValidateOperator(missingPartition));
    }

    @Test
    @DisplayName("invalid rank ranges are rejected")
    void testInvalidRankRange() {
        Map<String, Object> badFormat = validDescription();
        badFormat.put("rankRange", "rankEnd=3");
        assertFalse(strategy.executeValidateOperator(badFormat));

        Map<String, Object> zeroStart = validDescription();
        zeroStart.put("rankRange", "rankStart=0, rankEnd=3");
        assertFalse(strategy.executeValidateOperator(zeroStart));

        Map<String, Object> endLessThanStart = validDescription();
        endLessThanStart.put("rankRange", "rankStart=4, rankEnd=3");
        assertFalse(strategy.executeValidateOperator(endLessThanStart));

        Map<String, Object> unbounded = validDescription();
        unbounded.put("rankRange", "rankStart=1, rankEnd=9223372036854775807");
        assertFalse(strategy.executeValidateOperator(unbounded));
    }

    @Test
    @DisplayName("outputRankNumber=true is rejected (StreamExecLimit always passes false)")
    void testOutputRankNumberMustBeFalse() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("outputRankNumber", Boolean.TRUE);
        assertFalse(strategy.executeValidateOperator(jsonMap));

        Map<String, Object> missing = validDescription();
        missing.remove("outputRankNumber");
        assertFalse(strategy.executeValidateOperator(missing));
    }

    @Test
    @DisplayName("missing type lists and unsupported row types are rejected")
    void testTypes() {
        for (String field : Arrays.asList("inputTypes", "outputTypes")) {
            Map<String, Object> jsonMap = validDescription();
            jsonMap.remove(field);
            assertFalse(strategy.executeValidateOperator(jsonMap), "field: " + field);
        }
        // DOUBLE has no native output column branch in the Top-N family.
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("inputTypes", Arrays.asList("BIGINT", "DOUBLE"));
        assertFalse(strategy.executeValidateOperator(jsonMap));

        // VARCHAR (any precision) and TIMESTAMP(3) are supported.
        Map<String, Object> varchar = validDescription();
        varchar.put("inputTypes", Arrays.asList("VARCHAR(2000)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"));
        varchar.put("outputTypes", Arrays.asList("VARCHAR(2000)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"));
        assertTrue(strategy.executeValidateOperator(varchar));
    }

    @Test
    @DisplayName("factory registers the strategy under the Limit operator name")
    void testFactoryRegistration() {
        AbstractValidateOperatorStrategy registered = ValidateOperatorStrategyFactory.getStrategy("Limit");
        assertTrue(registered instanceof ValidateLimitOPStrategy);
    }
}
