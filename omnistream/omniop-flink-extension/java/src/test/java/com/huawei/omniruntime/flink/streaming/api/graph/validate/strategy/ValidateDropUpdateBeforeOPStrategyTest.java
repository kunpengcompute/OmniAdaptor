/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the DropUpdateBefore {@link ValidateDropUpdateBeforeOPStrategy}. */
class ValidateDropUpdateBeforeOPStrategyTest {

    private final ValidateDropUpdateBeforeOPStrategy strategy = new ValidateDropUpdateBeforeOPStrategy();

    private static Map<String, Object> validDescription() {
        // Mirrors the JSON written by the shadowed StreamExecDropUpdateBefore overlay.
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("operator", "DropUpdateBefore");
        jsonMap.put("originDescription", "DropUpdateBefore");
        jsonMap.put("inputTypes", Arrays.asList("BIGINT", "VARCHAR(20)"));
        jsonMap.put("outputTypes", Arrays.asList("BIGINT", "VARCHAR(20)"));
        return jsonMap;
    }

    @Test
    @DisplayName("valid description with operator marker and type lists passes validation")
    void testValidDescription() {
        assertTrue(strategy.executeValidateOperator(validDescription()));
    }

    @Test
    @DisplayName("missing or mismatched operator marker is rejected")
    void testOperatorMarker() {
        Map<String, Object> missing = validDescription();
        missing.remove("operator");
        assertFalse(strategy.executeValidateOperator(missing));

        Map<String, Object> lowercase = validDescription();
        lowercase.put("operator", "dropupdatebefore");
        assertFalse(strategy.executeValidateOperator(lowercase));

        Map<String, Object> other = validDescription();
        other.put("operator", "Calc");
        assertFalse(strategy.executeValidateOperator(other));
    }

    @Test
    @DisplayName("missing inputTypes or outputTypes is rejected")
    void testMissingTypeLists() {
        Map<String, Object> missingInput = validDescription();
        missingInput.remove("inputTypes");
        assertFalse(strategy.executeValidateOperator(missingInput));

        Map<String, Object> missingOutput = validDescription();
        missingOutput.remove("outputTypes");
        assertFalse(strategy.executeValidateOperator(missingOutput));
    }

    @Test
    @DisplayName("supported scalar and parameterized types pass validation")
    void testSupportedTypes() {
        List<String> types = Arrays.asList(
                "BIGINT",
                "INTEGER",
                "BOOLEAN",
                "VARCHAR(20)",
                "CHAR(1)",
                "DECIMAL64(10,2)",
                "DECIMAL128(20,2)",
                "TIME_WITHOUT_TIME_ZONE(3)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)",
                "DATE",
                "BIGINT NOT NULL");
        Map<String, Object> jsonMap = validDescription();
        jsonMap.put("inputTypes", types);
        jsonMap.put("outputTypes", types);
        assertTrue(strategy.executeValidateOperator(jsonMap));
    }

    @Test
    @DisplayName("unsupported types are rejected so the vertex falls back to Flink")
    void testUnsupportedTypes() {
        for (String type : Arrays.asList(
                "DOUBLE",
                "SMALLINT",
                "ARRAY",
                "BINARY",
                "VARBINARY(10)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(6)")) {
            Map<String, Object> jsonMap = validDescription();
            jsonMap.put("inputTypes", Collections.singletonList(type));
            jsonMap.put("outputTypes", Collections.singletonList(type));
            assertFalse(strategy.executeValidateOperator(jsonMap), "type: " + type);
        }
    }

    @Test
    @DisplayName("factory registers the strategy under the DropUpdateBefore operator name")
    void testFactoryRegistration() {
        AbstractValidateOperatorStrategy registered =
                ValidateOperatorStrategyFactory.getStrategy("DropUpdateBefore");
        assertTrue(registered instanceof ValidateDropUpdateBeforeOPStrategy);
    }
}
