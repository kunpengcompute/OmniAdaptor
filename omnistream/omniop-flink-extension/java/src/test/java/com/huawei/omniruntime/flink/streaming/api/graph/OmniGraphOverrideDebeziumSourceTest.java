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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the native debezium-json source contract. */
class OmniGraphOverrideDebeziumSourceTest {

    private static final List<String> COVERED_TYPES = Arrays.asList(
            "CHAR",
            "BIGINT",
            "INTEGER",
            "VARCHAR(20)",
            "BOOLEAN",
            "VARCHAR(2147483647)",
            "DATE",
            "TIME_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)");

    private static final List<String> NATIVE_METADATA = Arrays.asList(
            "schema",
            "ingestion-timestamp",
            "source.timestamp",
            "source.database",
            "source.schema",
            "source.table");

    private static Map<String, Object> description(List<String> types, List<String> metadata) {
        Map<String, Object> map = new HashMap<>();
        map.put("outputTypes", types);
        map.put("debeziumMetadata", metadata);
        return map;
    }

    @Test
    @DisplayName("covered scalar types and native metadata columns are accepted")
    void coveredTypesAndMetadataAreNative() {
        assertTrue(OmniGraphOverride.validateDebeziumSource(description(COVERED_TYPES, NATIVE_METADATA)));
    }

    @Test
    @DisplayName("source.properties (MAP) falls back to Flink")
    void sourcePropertiesFallsBack() {
        List<String> metadata = Arrays.asList(
                "schema",
                "ingestion-timestamp",
                "source.timestamp",
                "source.database",
                "source.schema",
                "source.table",
                "source.properties");
        assertFalse(OmniGraphOverride.validateDebeziumSource(description(COVERED_TYPES, metadata)));
    }

    @Test
    @DisplayName("unknown metadata keys fall back to Flink")
    void unknownMetadataFallsBack() {
        List<String> metadata = Arrays.asList("schema", "transaction-id");
        assertFalse(OmniGraphOverride.validateDebeziumSource(description(COVERED_TYPES, metadata)));
    }

    @Test
    @DisplayName("temporal precision above milliseconds falls back to Flink")
    void highPrecisionTemporalFallsBack() {
        assertTrue(OmniGraphOverride.validateDebeziumSource(
                description(Arrays.asList("TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                        "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)"), NATIVE_METADATA)));
        assertFalse(OmniGraphOverride.validateDebeziumSource(
                description(Arrays.asList("TIMESTAMP_WITHOUT_TIME_ZONE(6)"), NATIVE_METADATA)));
        assertFalse(OmniGraphOverride.validateDebeziumSource(
                description(Arrays.asList("TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)"), NATIVE_METADATA)));
        assertFalse(OmniGraphOverride.validateDebeziumSource(
                description(Arrays.asList("TIME_WITHOUT_TIME_ZONE(6)"), NATIVE_METADATA)));
    }

    @Test
    @DisplayName("missing output types fall back to Flink")
    void missingOutputTypesFallsBack() {
        Map<String, Object> map = new HashMap<>();
        map.put("debeziumMetadata", NATIVE_METADATA);
        assertFalse(OmniGraphOverride.validateDebeziumSource(map));
    }
}
