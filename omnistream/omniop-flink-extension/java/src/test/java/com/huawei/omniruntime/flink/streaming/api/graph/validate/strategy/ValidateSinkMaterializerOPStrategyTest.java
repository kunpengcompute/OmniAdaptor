/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
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

/** Unit tests for {@link ValidateSinkMaterializerOPStrategy}. */
class ValidateSinkMaterializerOPStrategyTest {

    private final ValidateSinkMaterializerOPStrategy strategy = new ValidateSinkMaterializerOPStrategy();

    private static Map<String, Object> validDescription() {
        // Mirrors the JSON written by the CommonExecSink materializer overlay.
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("originDescription", "SinkMaterializer(pk=[pk])");
        jsonMap.put("inputTypes", Arrays.asList("BIGINT", "BIGINT", "VARCHAR"));
        jsonMap.put("outputTypes", Arrays.asList("BIGINT", "BIGINT", "VARCHAR"));
        jsonMap.put("grouping", Arrays.asList(0));
        jsonMap.put("inputUpsertKey", null);
        return jsonMap;
    }

    @Test
    @DisplayName("materializer with primary key and no upsert key passes validation")
    void testValidDescription() {
        assertTrue(strategy.executeValidateOperator(validDescription()));
    }

    @Test
    @DisplayName("materializer with inputUpsertKey passes validation")
    void testValidUpsertKey() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("inputUpsertKey", Arrays.asList(1));
        assertTrue(strategy.executeValidateOperator(jsonMap));
    }

    @Test
    @DisplayName("missing or empty grouping is rejected")
    void testMissingOrEmptyGrouping() {
        Map<String, Object> missing = validDescription();
        missing.remove("grouping");
        assertFalse(strategy.executeValidateOperator(missing));

        Map<String, Object> empty = validDescription();
        empty.put("grouping", Arrays.asList());
        assertFalse(strategy.executeValidateOperator(empty));
    }

    @Test
    @DisplayName("grouping index out of range is rejected")
    void testGroupingIndexOutOfRange() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("grouping", Arrays.asList(3));
        assertFalse(strategy.executeValidateOperator(jsonMap));

        Map<String, Object> negative = validDescription();
        negative.put("grouping", Arrays.asList(-1));
        assertFalse(strategy.executeValidateOperator(negative));
    }

    @Test
    @DisplayName("non-list grouping or inputUpsertKey is rejected instead of throwing")
    void testNonListFields() {
        Map<String, Object> grouping = validDescription();
        grouping.put("grouping", "pk");
        assertFalse(strategy.executeValidateOperator(grouping));

        Map<String, Object> upsertKey = validDescription();
        upsertKey.put("inputUpsertKey", "uk");
        assertFalse(strategy.executeValidateOperator(upsertKey));
    }

    @Test
    @DisplayName("missing type lists are rejected")
    void testMissingTypeLists() {
        for (String field : Arrays.asList("inputTypes", "outputTypes")) {
            Map<String, Object> jsonMap = validDescription();
            jsonMap.remove(field);
            assertFalse(strategy.executeValidateOperator(jsonMap), "field: " + field);
        }
    }

    @Test
    @DisplayName("row types outside the native compare/copy whitelist are rejected")
    void testUnsupportedDataType() {
        // DECIMAL, TIMESTAMP_LTZ, TIME and FLOAT are not supported by the native
        // column compare/copy switch and must fall back to vanilla Flink.
        for (String badType : Arrays.asList("FLOAT", "DECIMAL64(10,2)", "DECIMAL128(20,2)",
                "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)", "TIME_WITHOUT_TIME_ZONE(3)", "DATE")) {
            Map<String, Object> jsonMap = validDescription();
            jsonMap.put("inputTypes", Arrays.asList("BIGINT", badType, "VARCHAR"));
            assertFalse(strategy.executeValidateOperator(jsonMap), "type: " + badType);
        }
    }

    @Test
    @DisplayName("whitelisted timestamp and varchar precisions pass validation")
    void testSupportedTimestampPrecisions() {
        for (String goodType : Arrays.asList("TIMESTAMP_WITHOUT_TIME_ZONE(0)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                "VARCHAR(2000)", "STRING", "BOOLEAN", "INTEGER", "DOUBLE", "CHAR")) {
            Map<String, Object> jsonMap = validDescription();
            jsonMap.put("inputTypes", Arrays.asList("BIGINT", goodType, "VARCHAR"));
            jsonMap.put("outputTypes", Arrays.asList("BIGINT", goodType, "VARCHAR"));
            assertTrue(strategy.executeValidateOperator(jsonMap), "type: " + goodType);
        }
    }

    @Test
    @DisplayName("inputUpsertKey index out of range is rejected")
    void testUpsertKeyIndexOutOfRange() {
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("inputUpsertKey", Arrays.asList(3));
        assertFalse(strategy.executeValidateOperator(jsonMap));
    }

    @Test
    @DisplayName("factory registers the strategy under the SinkMaterializer operator name")
    void testFactoryRegistration() {
        AbstractValidateOperatorStrategy registered =
                ValidateOperatorStrategyFactory.getStrategy("SinkMaterializer");
        assertTrue(registered instanceof ValidateSinkMaterializerOPStrategy);
    }
}
