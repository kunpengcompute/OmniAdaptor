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

/** Unit tests for the WindowJoin {@link ValidateWindowJoinOPStrategy}. */
class ValidateWindowJoinOPStrategyTest {

    private final ValidateWindowJoinOPStrategy strategy = new ValidateWindowJoinOPStrategy();

    private static final List<String> OUTER_JOIN_TYPES =
            Arrays.asList("LeftOuterJoin", "RightOuterJoin", "FullOuterJoin");

    private static Map<String, Object> description(String joinType, List<String> extraTypes) {
        return description(joinType, "BIGINT", extraTypes);
    }

    private static Map<String, Object> description(String joinType, String keyType, List<String> extraTypes) {
        // Baseline: one join key (index 0) of keyType and a BIGINT window-end column (index 1).
        List<String> inputs = new ArrayList<>(Arrays.asList(keyType, "BIGINT"));
        inputs.addAll(extraTypes);

        List<String> outputs = new ArrayList<>(inputs);
        outputs.addAll(inputs);

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("joinType", joinType);
        jsonMap.put("leftTimeType", "event");
        jsonMap.put("rightTimeType", "event");
        jsonMap.put("leftJoinKey", new ArrayList<>(Collections.singletonList(0)));
        jsonMap.put("rightJoinKey", new ArrayList<>(Collections.singletonList(0)));
        jsonMap.put("leftInputTypes", inputs);
        jsonMap.put("rightInputTypes", new ArrayList<>(inputs));
        jsonMap.put("outputTypes", outputs);
        jsonMap.put("leftWindowEndIndex", 1);
        jsonMap.put("rightWindowEndIndex", 1);
        return jsonMap;
    }

    @Test
    @DisplayName("baseline outer-join descriptions pass validation")
    void testValidDescription() {
        for (String joinType : OUTER_JOIN_TYPES) {
            assertTrue(strategy.executeValidateOperator(
                    description(joinType, Collections.emptyList())), "joinType: " + joinType);
        }
    }

    @Test
    @DisplayName("all outer joins accept the covered ordinary-column types")
    void testCoveredTypes() {
        List<String> types = Arrays.asList(
                "CHAR",
                "VARCHAR(9)",
                "STRING",
                "BOOLEAN",
                "DATE",
                "TIME_WITHOUT_TIME_ZONE(0)",
                "TIME_WITHOUT_TIME_ZONE(3)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(0)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(9)",
                "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)");
        for (String joinType : OUTER_JOIN_TYPES) {
            assertTrue(strategy.executeValidateOperator(description(joinType, types)),
                    "joinType: " + joinType);
        }
    }

    @Test
    @DisplayName("all outer joins fall back for DECIMAL columns")
    void testDecimalFallsBack() {
        for (String joinType : OUTER_JOIN_TYPES) {
            for (String type : Arrays.asList("DECIMAL64(10,2)", "DECIMAL128(38,10)")) {
                assertFalse(strategy.executeValidateOperator(
                                description(joinType, Collections.singletonList(type))),
                        "joinType: " + joinType + ", type: " + type);
            }
        }
    }

    @Test
    @DisplayName("all outer joins fall back for TIMESTAMP WITH TIME ZONE")
    void testTimestampWithTimeZoneFallsBack() {
        for (String joinType : OUTER_JOIN_TYPES) {
            assertFalse(strategy.executeValidateOperator(
                            description(joinType, Collections.singletonList("TIMESTAMP_WITH_TIME_ZONE(3)"))),
                    "joinType: " + joinType);
        }
    }

    @Test
    @DisplayName("all outer joins fall back for INTERVAL and container types")
    void testIntervalAndContainerFallBack() {
        for (String joinType : OUTER_JOIN_TYPES) {
            for (String type : Arrays.asList(
                    "INTERVAL_YEAR_MONTH",
                    "INTERVAL_DAY_TIME",
                    "ARRAY<INT>",
                    "MAP<STRING, BIGINT>",
                    "ROW<f INT>",
                    "RAW('class', 'snapshot')")) {
                assertFalse(strategy.executeValidateOperator(
                                description(joinType, Collections.singletonList(type))),
                        "joinType: " + joinType + ", type: " + type);
            }
        }
    }

    @Test
    @DisplayName("all outer joins use a strict whitelist for other types")
    void testStrictWhitelist() {
        for (String joinType : OUTER_JOIN_TYPES) {
            for (String type : Arrays.asList(
                    "DOUBLE",
                    "SMALLINT",
                    "TINYINT",
                    "FLOAT",
                    "BINARY(10)",
                    "VARBINARY(10)",
                    "TIMESTAMP_WITHOUT_TIME_ZONE(6)")) {
                assertFalse(strategy.executeValidateOperator(
                                description(joinType, Collections.singletonList(type))),
                        "joinType: " + joinType + ", type: " + type);
            }
        }
    }

    @Test
    @DisplayName("the outer-join policy applies to all outer joins but not InnerJoin")
    void testPolicyAppliesToAllOuterJoins() {
        // Date is accepted for every outer join, DECIMAL falls back for every outer join.
        for (String joinType : OUTER_JOIN_TYPES) {
            assertTrue(strategy.executeValidateOperator(
                            description(joinType, Collections.singletonList("DATE"))),
                    "joinType: " + joinType);
            assertFalse(strategy.executeValidateOperator(
                            description(joinType, Collections.singletonList("DECIMAL64(10,2)"))),
                    "joinType: " + joinType);
        }
        // DECIMAL and DATE keep the previous behaviour for InnerJoin.
        assertTrue(strategy.executeValidateOperator(
                description("InnerJoin", Collections.singletonList("DECIMAL64(10,2)"))));
        assertFalse(strategy.executeValidateOperator(
                description("InnerJoin", Collections.singletonList("DATE"))));
    }

    @Test
    @DisplayName("equi join keys are limited to BIGINT and string")
    void testJoinKeyOnlyBigintOrString() {
        for (String keyType : Arrays.asList(
                "BIGINT",
                "BIGINT NOT NULL",
                "VARCHAR(9)",
                "VARCHAR(2147483647)",
                "VARCHAR(9) NOT NULL",
                "STRING")) {
            assertTrue(strategy.executeValidateOperator(
                            description("LeftOuterJoin", keyType, Collections.emptyList())),
                    "keyType: " + keyType);
        }

        for (String keyType : Arrays.asList(
                "INTEGER",
                "INT",
                "CHAR",
                "CHAR(1)",
                "BOOLEAN",
                "DATE",
                "TIME_WITHOUT_TIME_ZONE(3)",
                "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)",
                "DECIMAL64(10,2)",
                "DECIMAL128(38,10)")) {
            assertFalse(strategy.executeValidateOperator(
                            description("LeftOuterJoin", keyType, Collections.emptyList())),
                    "keyType: " + keyType);
        }
    }

    // ---------------------------------------------------------------------
    // Non-equi native envelope (strict BINARY + BOOLEAN, delegated to Calc).
    // ---------------------------------------------------------------------

    private static Map<String, Object> nonEquiDescription(
            String joinType,
            List<String> leftInputs,
            List<String> rightInputs,
            Map<String, Object> nonEquiCondition) {
        List<String> outputs = new ArrayList<>(leftInputs);
        outputs.addAll(rightInputs);

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("joinType", joinType);
        jsonMap.put("leftTimeType", "event");
        jsonMap.put("rightTimeType", "event");
        jsonMap.put("leftJoinKey", new ArrayList<>(Collections.singletonList(0)));
        jsonMap.put("rightJoinKey", new ArrayList<>(Collections.singletonList(0)));
        jsonMap.put("leftInputTypes", leftInputs);
        jsonMap.put("rightInputTypes", rightInputs);
        jsonMap.put("outputTypes", outputs);
        jsonMap.put("leftWindowEndIndex", 1);
        jsonMap.put("rightWindowEndIndex", 1);
        jsonMap.put("nonEquiCondition", nonEquiCondition);
        return jsonMap;
    }

    private static Map<String, Object> nonEquiDescription(
            String valueType, Map<String, Object> nonEquiCondition) {
        return nonEquiDescription("LeftOuterJoin", valueType, nonEquiCondition);
    }

    private static Map<String, Object> nonEquiDescription(
            String joinType, String valueType, Map<String, Object> nonEquiCondition) {
        List<String> inputs = Arrays.asList("BIGINT", "BIGINT", valueType);
        return nonEquiDescription(joinType, inputs, new ArrayList<>(inputs), nonEquiCondition);
    }

    private static Map<String, Object> field(int dataTypeId, int colVal) {
        Map<String, Object> field = new HashMap<>();
        field.put("exprType", "FIELD_REFERENCE");
        field.put("dataType", dataTypeId);
        field.put("colVal", colVal);
        return field;
    }

    private static Map<String, Object> literal(int dataTypeId, Object value) {
        Map<String, Object> literal = new HashMap<>();
        literal.put("exprType", "LITERAL");
        literal.put("dataType", dataTypeId);
        literal.put("isNull", value == null);
        if (value != null) {
            literal.put("value", value);
        }
        return literal;
    }

    private static Map<String, Object> binary(
            String exprType, String operator, int returnType, Object left, Object right) {
        Map<String, Object> node = new HashMap<>();
        node.put("exprType", exprType);
        node.put("operator", operator);
        node.put("returnType", returnType);
        node.put("left", left);
        node.put("right", right);
        return node;
    }

    private static Map<String, Object> comparison(String operator, Object left, Object right) {
        return binary("BINARY", operator, 4, left, right);
    }

    private static Map<String, Object> not(Object expr) {
        Map<String, Object> node = new HashMap<>();
        node.put("exprType", "UNARY");
        node.put("operator", "NOT");
        node.put("returnType", 4);
        node.put("expr", expr);
        return node;
    }

    @Test
    @DisplayName("non-equi supports the covered column types")
    void testNonEquiSupportedTypes() {
        String[][] cases = {
                {"INTEGER", "1"},
                {"BIGINT", "2"},
                {"CHAR", "16"},
                {"VARCHAR(9)", "15"},
                {"VARCHAR(2147483647)", "15"}, // STRING
                {"DATE", "1"},
                {"TIME_WITHOUT_TIME_ZONE(3)", "2"},
                {"TIMESTAMP_WITHOUT_TIME_ZONE(3)", "2"},
                {"TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)", "2"},
        };
        for (String[] typeAndId : cases) {
            int dataTypeId = Integer.parseInt(typeAndId[1]);
            Map<String, Object> condition = comparison(
                    "LESS_THAN", field(dataTypeId, 2), field(dataTypeId, 5));
            assertTrue(
                    strategy.executeValidateOperator(nonEquiDescription(typeAndId[0], condition)),
                    "type: " + typeAndId[0]);
        }
    }

    @Test
    @DisplayName("non-equi supports all six comparison operators")
    void testNonEquiComparisonOperators() {
        for (String operator : Arrays.asList(
                "EQUAL", "NOT_EQUAL", "GREATER_THAN", "GREATER_THAN_OR_EQUAL",
                "LESS_THAN", "LESS_THAN_OR_EQUAL")) {
            Map<String, Object> condition = comparison(operator, field(1, 2), field(1, 5));
            assertTrue(
                    strategy.executeValidateOperator(nonEquiDescription("INTEGER", condition)),
                    "operator: " + operator);
        }
    }

    @Test
    @DisplayName("non-equi delegates known type ids to the Calc validator")
    void testNonEquiCalcExprAcceptsKnownTypeIds() {
        // The declared columns stay BIGINT so the ordinary-column policy cannot reject the case;
        // only the JSON dataType id of the comparison operands varies.
        String[][] cases = {
                {"DECIMAL64(10,2)", "6"},
                {"DECIMAL128(38,10)", "7"},
                {"DOUBLE", "3"},
                {"SMALLINT", "5"},
                {"TIMESTAMP_WITHOUT_TIME_ZONE(9)", "2"},
                {"BOOLEAN", "4"},
        };
        for (String[] typeAndId : cases) {
            int dataTypeId = Integer.parseInt(typeAndId[1]);
            Map<String, Object> condition = comparison(
                    "LESS_THAN", field(dataTypeId, 2), field(dataTypeId, 5));
            assertTrue(
                    strategy.executeValidateOperator(nonEquiDescription("BIGINT", condition)),
                    "typeId: " + typeAndId[0]);
        }
    }

    @Test
    @DisplayName("LeftOuterJoin ordinary-column policy is independent of the non-equi check")
    void testLeftOuterColumnPolicyStillApplies() {
        Map<String, Object> condition = comparison("LESS_THAN", field(6, 2), field(6, 5));
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("DECIMAL64(10,2)", condition)));
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("DOUBLE", condition)));
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("SMALLINT", condition)));
        // TIMESTAMP(9) is allowed as an ordinary column, so only the non-equi gate decides.
        assertTrue(strategy.executeValidateOperator(
                nonEquiDescription("TIMESTAMP_WITHOUT_TIME_ZONE(9)", condition)));
    }

    @Test
    @DisplayName("non-equi ordinary-column policy is identical for all outer joins")
    void testNonEquiOuterJoinPolicyConsistent() {
        Map<String, Object> dateCondition = comparison("LESS_THAN", field(8, 2), field(8, 5));
        Map<String, Object> decimalCondition = comparison("LESS_THAN", field(6, 2), field(6, 5));
        for (String joinType : OUTER_JOIN_TYPES) {
            assertTrue(strategy.executeValidateOperator(
                            nonEquiDescription(joinType, "DATE", dateCondition)),
                    "joinType: " + joinType + ", type: DATE");
            assertFalse(strategy.executeValidateOperator(
                            nonEquiDescription(joinType, "DECIMAL64(10,2)", decimalCondition)),
                    "joinType: " + joinType + ", type: DECIMAL64(10,2)");
        }
    }

    @Test
    @DisplayName("non-equi no longer checks the actual type family behind a physical alias")
    void testNonEquiAliasCollisionNoLongerChecked() {
        List<String> leftInputs = Arrays.asList("BIGINT", "BIGINT", "TIME_WITHOUT_TIME_ZONE(3)");
        List<String> rightInputs = Arrays.asList("BIGINT", "BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)");
        Map<String, Object> condition = comparison("LESS_THAN", field(2, 2), field(2, 5));
        assertTrue(strategy.executeValidateOperator(
                nonEquiDescription("LeftOuterJoin", leftInputs, rightInputs, condition)));
    }

    @Test
    @DisplayName("non-equi binary AND/OR pass; top-level NOT and MULTIPLE_AND_OR fall back")
    void testNonEquiLogicalExpressions() {
        Map<String, Object> lt = comparison("LESS_THAN", field(1, 2), field(1, 5));
        Map<String, Object> gt = comparison("GREATER_THAN", field(1, 2), field(1, 5));
        Map<String, Object> eq = comparison("EQUAL", field(1, 2), literal(1, 5));

        assertTrue(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", binary("BINARY", "AND", 4, lt, gt))));
        assertTrue(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", binary("BINARY", "OR", 4, lt, eq))));
        // Top-level UNARY / MULTIPLE_AND_OR are not BINARY and therefore fall back.
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", not(gt))));

        Map<String, Object> multiple = new HashMap<>();
        multiple.put("exprType", "MULTIPLE_AND_OR");
        multiple.put("operator", "AND");
        multiple.put("returnType", 4);
        multiple.put("conditions", Arrays.asList(lt, gt));
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", multiple)));
    }

    @Test
    @DisplayName("non-equi top-level expression must be BINARY")
    void testNonEquiTopLevelMustBeBinary() {
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", literal(1, 2))));
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", field(1, 2))));
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", not(comparison("LESS_THAN", field(1, 2), field(1, 5))))));

        Map<String, Object> function = new HashMap<>();
        function.put("exprType", "FUNCTION");
        function.put("function_name", "CAST");
        function.put("returnType", 1);
        function.put("arguments", Collections.singletonList(field(1, 2)));
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", function)));

        Map<String, Object> inNode = new HashMap<>();
        inNode.put("exprType", "IN");
        inNode.put("returnType", 4);
        inNode.put("arguments", Arrays.asList(field(1, 2), literal(1, 1)));
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", inNode)));

        Map<String, Object> betweenNode = new HashMap<>();
        betweenNode.put("exprType", "BETWEEN");
        betweenNode.put("returnType", 4);
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", betweenNode)));
    }

    @Test
    @DisplayName("non-equi return type must be BOOLEAN")
    void testNonEquiReturnTypeMustBeBoolean() {
        Map<String, Object> condition = binary("BINARY", "LESS_THAN", 1, field(1, 2), field(1, 5));
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", condition)));
    }

    @Test
    @DisplayName("non-equi field references must stay within the combined input width")
    void testNonEquiFieldReferenceOutOfBound() {
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", comparison("LESS_THAN", field(1, 2), field(1, 6)))));
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", comparison("LESS_THAN", field(1, -1), field(1, 5)))));
    }

    @Test
    @DisplayName("CAST and arithmetic operands are accepted by the Calc validator")
    void testNonEquiCastAndArithmeticOperandsAccepted() {
        Map<String, Object> castField = new HashMap<>();
        castField.put("exprType", "FUNCTION");
        castField.put("function_name", "CAST");
        castField.put("returnType", 1);
        castField.put("arguments", Collections.singletonList(field(2, 2)));
        assertTrue(strategy.executeValidateOperator(nonEquiDescription(
                "INTEGER", comparison("LESS_THAN", castField, field(1, 5)))));

        Map<String, Object> arithmetic = binary("BINARY", "ADD", 1, field(1, 2), literal(1, 1));
        assertTrue(strategy.executeValidateOperator(nonEquiDescription(
                "INTEGER", comparison("LESS_THAN", arithmetic, field(1, 5)))));
    }

    @Test
    @DisplayName("LIKE/IN/BETWEEN fall back to Flink")
    void testNonEquiLikeInBetweenFallBack() {
        assertFalse(strategy.executeValidateOperator(nonEquiDescription(
                "VARCHAR(9)", comparison("LIKE", field(15, 2), field(15, 5)))));

        Map<String, Object> inNode = new HashMap<>();
        inNode.put("exprType", "IN");
        inNode.put("returnType", 4);
        inNode.put("arguments", Arrays.asList(field(1, 2), literal(1, 1)));
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", inNode)));

        Map<String, Object> betweenNode = new HashMap<>();
        betweenNode.put("exprType", "BETWEEN");
        betweenNode.put("returnType", 4);
        assertFalse(strategy.executeValidateOperator(
                nonEquiDescription("INTEGER", betweenNode)));
    }

    @Test
    @DisplayName("factory registers the strategy under the WindowJoin operator name")
    void testFactoryRegistration() {
        AbstractValidateOperatorStrategy registered =
                ValidateOperatorStrategyFactory.getStrategy("WindowJoin");
        assertTrue(registered instanceof ValidateWindowJoinOPStrategy);
    }
}
