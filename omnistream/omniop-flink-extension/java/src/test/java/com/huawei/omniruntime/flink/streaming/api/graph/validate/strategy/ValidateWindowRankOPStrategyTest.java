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
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the window TVF Top-N {@link ValidateWindowRankOPStrategy}. */
class ValidateWindowRankOPStrategyTest {

    private final ValidateWindowRankOPStrategy strategy = new ValidateWindowRankOPStrategy();

    private static Map<String, Object> validDescription() {
        // Mirrors the JSON written by the StreamExecWindowRank overlay for
        // ROW_NUMBER() OVER (PARTITION BY window_start, window_end, category
        // ORDER BY val DESC) WHERE rn <= 2 over a TUMBLE 5s window.
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("originDescription", "WindowRank(...)");
        jsonMap.put("isWindowRank", Boolean.TRUE);
        jsonMap.put("inputTypes", Arrays.asList(
                "BIGINT", "BIGINT", "BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"));
        jsonMap.put("outputTypes", Arrays.asList("BIGINT", "BIGINT", "BIGINT", "BIGINT"));
        jsonMap.put("partitionKey", Arrays.asList(0, 4, 5));
        jsonMap.put("sortFieldIndices", Arrays.asList(2));
        jsonMap.put("sortAscendingOrders", Arrays.asList(Boolean.FALSE));
        jsonMap.put("sortNullsIsLast", Arrays.asList(Boolean.TRUE));
        jsonMap.put("rankStart", 1L);
        jsonMap.put("rankEnd", 2L);
        jsonMap.put("outputRankNumber", Boolean.TRUE);
        jsonMap.put("windowEndIndex", 5);
        jsonMap.put("shiftTimeZone", "UTC");
        return jsonMap;
    }

    @Test
    @DisplayName("valid window Top-N description passes validation")
    void testValidWindowRank() {
        assertTrue(strategy.executeValidateOperator(validDescription()));
    }

    @Test
    @DisplayName("offset rank range (rn between 2 and 3) passes validation")
    void testValidOffsetRankRange() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("rankStart", 2L);
        jsonMap.put("rankEnd", 3L);
        assertTrue(strategy.executeValidateOperator(jsonMap));
    }

    @Test
    @DisplayName("supported type matrix passes validation")
    void testValidTypeMatrix() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("inputTypes", Arrays.asList(
                "CHAR(1)", "BIGINT", "INTEGER", "VARCHAR", "BOOLEAN", "STRING",
                "DATE", "TIME_WITHOUT_TIME_ZONE(0)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"));
        jsonMap.put("outputTypes", Arrays.asList(
                "CHAR(1)", "BIGINT", "INTEGER", "VARCHAR", "BOOLEAN", "STRING",
                "DATE", "TIME_WITHOUT_TIME_ZONE(0)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                "BIGINT"));
        jsonMap.put("sortFieldIndices", Arrays.asList(1, 2));
        jsonMap.put("sortAscendingOrders", Arrays.asList(Boolean.TRUE, Boolean.FALSE));
        jsonMap.put("sortNullsIsLast", Arrays.asList(Boolean.TRUE, Boolean.FALSE));
        jsonMap.put("windowEndIndex", 9);
        jsonMap.put("partitionKey", Arrays.asList(0, 3, 5));
        assertTrue(strategy.executeValidateOperator(jsonMap));
    }

    @Test
    @DisplayName("description without the isWindowRank marker is rejected")
    void testMissingMarker() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.remove("isWindowRank");
        assertFalse(strategy.executeValidateOperator(jsonMap));

        Map<String, Object> falseMarker = validDescription();
        falseMarker.put("isWindowRank", Boolean.FALSE);
        assertFalse(strategy.executeValidateOperator(falseMarker));
    }

    @Test
    @DisplayName("mismatched sort contract arrays are rejected")
    void testMismatchedSortContract() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("sortFieldIndices", Arrays.asList(2, 1));
        assertFalse(strategy.executeValidateOperator(jsonMap));

        Map<String, Object> emptySort = validDescription();
        emptySort.put("sortFieldIndices", java.util.Collections.emptyList());
        emptySort.put("sortAscendingOrders", java.util.Collections.emptyList());
        emptySort.put("sortNullsIsLast", java.util.Collections.emptyList());
        assertFalse(strategy.executeValidateOperator(emptySort));
    }

    @Test
    @DisplayName("illegal rank ranges are rejected")
    void testIllegalRankRange() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("rankStart", 0L);
        assertFalse(strategy.executeValidateOperator(jsonMap));

        Map<String, Object> reversed = validDescription();
        reversed.put("rankStart", 3L);
        reversed.put("rankEnd", 2L);
        assertFalse(strategy.executeValidateOperator(reversed));
    }

    @Test
    @DisplayName("window end column with precision != 3 is rejected")
    void testWindowEndPrecision() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("inputTypes", Arrays.asList(
                "BIGINT", "BIGINT", "BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(6)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(6)", "TIMESTAMP_WITHOUT_TIME_ZONE(6)"));
        assertFalse(strategy.executeValidateOperator(jsonMap));
    }

    @Test
    @DisplayName("unsupported column types fall back to vanilla Flink")
    void testUnsupportedTypes() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("inputTypes", Arrays.asList(
                "BIGINT", "BIGINT", "DOUBLE", "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"));
        jsonMap.put("sortFieldIndices", Arrays.asList(2));
        assertFalse(strategy.executeValidateOperator(jsonMap));

        Map<String, Object> missingTypes = validDescription();
        missingTypes.remove("inputTypes");
        assertFalse(strategy.executeValidateOperator(missingTypes));
    }
}
