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

package com.huawei.omniruntime.flink.streaming.api.graph;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the source/sink-shared upsert-kafka key validation. */
class OmniGraphOverrideUpsertKafkaKeyTest {

    private static Map<String, Object> description(List<Integer> keys, List<String> types) {
        Map<String, Object> map = new HashMap<>();
        map.put("keyProjection", keys);
        map.put("outputTypes", types);
        map.put("physicalTypes", types);
        return map;
    }

    @Test
    @DisplayName("source: single BIGINT key passes")
    void sourceSingleBigint() {
        assertTrue(OmniGraphOverride.validateUpsertKafkaKey(
                description(Arrays.asList(0), Arrays.asList("BIGINT", "BIGINT")), "outputTypes"));
    }

    @Test
    @DisplayName("source: composite key passes")
    void sourceComposite() {
        assertTrue(OmniGraphOverride.validateUpsertKafkaKey(
                description(Arrays.asList(0, 1), Arrays.asList("BIGINT", "VARCHAR(10)", "BIGINT")), "outputTypes"));
    }

    @Test
    @DisplayName("source: non-BIGINT (STRING/VARCHAR) key passes")
    void sourceNonBigint() {
        assertTrue(OmniGraphOverride.validateUpsertKafkaKey(
                description(Arrays.asList(0), Arrays.asList("VARCHAR(10)", "BIGINT")), "outputTypes"));
    }

    @Test
    @DisplayName("sink: single BIGINT key with NOT NULL suffix passes")
    void sinkSingleBigint() {
        assertTrue(OmniGraphOverride.validateUpsertKafkaKey(
                description(Arrays.asList(0), Arrays.asList("BIGINT NOT NULL", "BIGINT")), "physicalTypes"));
    }

    @Test
    @DisplayName("sink: composite key passes")
    void sinkComposite() {
        assertTrue(OmniGraphOverride.validateUpsertKafkaKey(
                description(Arrays.asList(0, 2), Arrays.asList("BIGINT", "BOOLEAN", "VARCHAR(20)")), "physicalTypes"));
    }

    @Test
    @DisplayName("value/non-key column types are not restricted here")
    void valueTypeNotRestricted() {
        assertTrue(OmniGraphOverride.validateUpsertKafkaKey(
                description(Arrays.asList(0), Arrays.asList("BIGINT", "DOUBLE")), "outputTypes"));
    }

    @Test
    @DisplayName("empty keyProjection rejected")
    void emptyKeysRejected() {
        assertFalse(OmniGraphOverride.validateUpsertKafkaKey(
                description(Collections.emptyList(), Arrays.asList("BIGINT", "BIGINT")), "outputTypes"));
    }

    @Test
    @DisplayName("non-Integer key index rejected")
    void nonIntegerIndexRejected() {
        Map<String, Object> map = new HashMap<>();
        map.put("keyProjection", Arrays.asList("0"));
        map.put("outputTypes", Arrays.asList("BIGINT", "BIGINT"));
        map.put("physicalTypes", Arrays.asList("BIGINT", "BIGINT"));
        assertFalse(OmniGraphOverride.validateUpsertKafkaKey(map, "outputTypes"));
    }

    @Test
    @DisplayName("out-of-range key index rejected")
    void outOfRangeRejected() {
        assertFalse(OmniGraphOverride.validateUpsertKafkaKey(
                description(Arrays.asList(5), Arrays.asList("BIGINT", "BIGINT")), "outputTypes"));
    }

    @Test
    @DisplayName("DOUBLE / DECIMAL key types rejected")
    void unsupportedTypesRejected() {
        assertFalse(OmniGraphOverride.validateUpsertKafkaKey(
                description(Arrays.asList(0), Arrays.asList("DOUBLE", "BIGINT")), "outputTypes"));
        assertFalse(OmniGraphOverride.validateUpsertKafkaKey(
                description(Arrays.asList(0), Arrays.asList("DECIMAL64(10,2)", "BIGINT")), "outputTypes"));
    }

    @Test
    @DisplayName("TIME(>0) / TIMESTAMP(>3) keys rejected")
    void timePrecisionsRejected() {
        assertFalse(OmniGraphOverride.validateUpsertKafkaKey(
                description(Arrays.asList(0), Arrays.asList("TIME_WITHOUT_TIME_ZONE(1)", "BIGINT")), "outputTypes"));
        assertFalse(OmniGraphOverride.validateUpsertKafkaKey(
                description(Arrays.asList(0), Arrays.asList("TIMESTAMP_WITHOUT_TIME_ZONE(9)", "BIGINT")), "outputTypes"));
    }

    @Test
    @DisplayName("missing keyProjection / missing types rejected")
    void missingFieldsRejected() {
        Map<String, Object> noProjection = new HashMap<>();
        noProjection.put("outputTypes", Arrays.asList("BIGINT"));
        noProjection.put("physicalTypes", Arrays.asList("BIGINT"));
        assertFalse(OmniGraphOverride.validateUpsertKafkaKey(noProjection, "outputTypes"));

        Map<String, Object> noTypes = new HashMap<>();
        noTypes.put("keyProjection", Arrays.asList(0));
        assertFalse(OmniGraphOverride.validateUpsertKafkaKey(noTypes, "outputTypes"));
    }
}
