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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the WindowTableFunction {@link ValidateWindowTableFunctionOPStrategy}. */
class ValidateWindowTableFunctionOPStrategyTest {

    private final ValidateWindowTableFunctionOPStrategy strategy =
            new ValidateWindowTableFunctionOPStrategy();

    private static Map<String, Object> validDescription() {
        // Mirrors the JSON written by the shadowed StreamExecWindowTableFunction overlay:
        // TUMBLE over event time with a TIMESTAMP(3) time attribute.
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("window", "TUMBLE(ROW_TIME, 10)");
        jsonMap.put("timeType", "event");
        jsonMap.put("timeAttributeType", "TIMESTAMP_WITHOUT_TIME_ZONE(3)");
        jsonMap.put("timeAttributeIndex", 2);
        jsonMap.put("inputTypes", new ArrayList<>(Arrays.asList(
                "VARCHAR(9)", "INTEGER", "TIMESTAMP_WITHOUT_TIME_ZONE(3)")));
        jsonMap.put("outputTypes", new ArrayList<>(Arrays.asList(
                "VARCHAR(9)", "INTEGER", "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(3)")));
        return jsonMap;
    }

    private static Map<String, Object> withTypes(Map<String, Object> jsonMap, List<String> types) {
        // Keep the baseline inputs so that timeAttributeIndex=2 still points at
        // TIMESTAMP_WITHOUT_TIME_ZONE(3), then append the types under test.
        List<String> inputs = new ArrayList<>(Arrays.asList(
                "VARCHAR(9)", "INTEGER", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"));
        inputs.addAll(types);

        List<String> outputs = new ArrayList<>(inputs);
        outputs.add("TIMESTAMP_WITHOUT_TIME_ZONE(3)");
        outputs.add("TIMESTAMP_WITHOUT_TIME_ZONE(3)");
        outputs.add("TIMESTAMP_WITHOUT_TIME_ZONE(3)");
        jsonMap.put("inputTypes", inputs);
        jsonMap.put("outputTypes", outputs);
        return jsonMap;
    }

    @Test
    @DisplayName("valid event time TUMBLE description passes validation")
    void testValidDescription() {
        assertTrue(strategy.executeValidateOperator(validDescription()));
    }

    @Test
    @DisplayName("missing or empty inputTypes is rejected")
    void testMissingInputTypes() {
        Map<String, Object> missing = validDescription();
        missing.remove("inputTypes");
        assertFalse(strategy.executeValidateOperator(missing));

        Map<String, Object> empty = validDescription();
        empty.put("inputTypes", Collections.emptyList());
        assertFalse(strategy.executeValidateOperator(empty));
    }

    @Test
    @DisplayName("all covered types (including DATE) pass validation")
    void testAllCoveredTypes() {
        List<String> types = Arrays.asList(
                "VARCHAR(9)",
                "CHAR(1)",
                "BIGINT",
                "INTEGER",
                "BOOLEAN",
                "STRING",
                "DATE",
                "TIME_WITHOUT_TIME_ZONE(0)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)");
        assertTrue(strategy.executeValidateOperator(withTypes(validDescription(), types)));
    }

    @Test
    @DisplayName("DATE is accepted as an operator-specific extra type")
    void testDateAllowed() {
        assertTrue(strategy.executeValidateOperator(
                withTypes(validDescription(), Collections.singletonList("DATE"))));
    }

    @Test
    @DisplayName("unsupported types are rejected so the vertex falls back to Flink")
    void testUnsupportedTypes() {
        for (String type : Arrays.asList(
                "DOUBLE",
                "SMALLINT",
                "VARBINARY(10)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(6)")) {
            assertFalse(strategy.executeValidateOperator(
                            withTypes(validDescription(), Collections.singletonList(type))),
                    "type: " + type);
        }
    }

    @Test
    @DisplayName("unsupported window types are rejected")
    void testUnsupportedWindowType() {
        Map<String, Object> session = validDescription();
        session.put("window", "SESSION(ROW_TIME, 10)");
        assertFalse(strategy.executeValidateOperator(session));
    }

    @Test
    @DisplayName("time attribute type must be TIMESTAMP(3) or TIMESTAMP_LTZ(3)")
    void testTimeAttributeType() {
        Map<String, Object> bad = validDescription();
        bad.put("timeAttributeType", "TIMESTAMP_WITHOUT_TIME_ZONE(6)");
        assertFalse(strategy.executeValidateOperator(bad));

        Map<String, Object> ltz = validDescription();
        ltz.put("timeType", "processing");
        ltz.put("timeAttributeType", "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)");
        ltz.remove("timeAttributeIndex");
        assertTrue(strategy.executeValidateOperator(ltz));
    }

    @Test
    @DisplayName("event time requires an in-range timeAttributeIndex")
    void testTimeAttributeIndex() {
        Map<String, Object> missing = validDescription();
        missing.remove("timeAttributeIndex");
        assertFalse(strategy.executeValidateOperator(missing));

        Map<String, Object> negative = validDescription();
        negative.put("timeAttributeIndex", -1);
        assertFalse(strategy.executeValidateOperator(negative));

        Map<String, Object> outOfRange = validDescription();
        outOfRange.put("timeAttributeIndex", 3);
        assertFalse(strategy.executeValidateOperator(outOfRange));
    }

    @Test
    @DisplayName("HOP requires windowSlide and CUMULATE requires maxSize/step")
    void testWindowParameters() {
        Map<String, Object> hop = validDescription();
        hop.put("window", "HOP(ROW_TIME, 10, 5)");
        assertFalse(strategy.executeValidateOperator(hop));
        hop.put("windowSlide", 5);
        assertTrue(strategy.executeValidateOperator(hop));

        Map<String, Object> cumulate = validDescription();
        cumulate.put("window", "CUMULATE(ROW_TIME, 10, 100)");
        assertFalse(strategy.executeValidateOperator(cumulate));
        cumulate.put("maxSize", 100);
        cumulate.put("step", 10);
        assertTrue(strategy.executeValidateOperator(cumulate));
    }

    @Test
    @DisplayName("factory registers the strategy under the WindowTableFunction operator name")
    void testFactoryRegistration() {
        AbstractValidateOperatorStrategy registered =
                ValidateOperatorStrategyFactory.getStrategy("WindowTableFunction");
        assertTrue(registered instanceof ValidateWindowTableFunctionOPStrategy);
    }
}
