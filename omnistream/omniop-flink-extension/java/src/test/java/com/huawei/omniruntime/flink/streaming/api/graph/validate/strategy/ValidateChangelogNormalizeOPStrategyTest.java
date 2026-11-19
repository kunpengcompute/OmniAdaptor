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

/** Unit tests for {@link ValidateChangelogNormalizeOPStrategy} key-shape rules. */
class ValidateChangelogNormalizeOPStrategyTest {

    private static Map<String, Object> validDescription() {
        Map<String, Object> m = new HashMap<>();
        m.put("processFunction", "ProcTimeDeduplicateKeepLastRowFunction");
        m.put("inputInsertOnly", Boolean.FALSE);
        m.put("generateInsert", Boolean.TRUE);
        m.put("generateUpdateBefore", Boolean.FALSE);
        m.put("miniBatchEnabled", Boolean.FALSE);
        m.put("stateRetentionTime", 0L);
        m.put("inputTypes", Arrays.asList("BIGINT", "BIGINT"));
        m.put("grouping", Arrays.asList(0));
        return m;
    }

    @Test
    @DisplayName("supportsGroupKeyColumns: single key passes")
    void singleKeySupported() {
        assertTrue(ValidateChangelogNormalizeOPStrategy.supportsGroupKeyColumns(
                Arrays.asList("BIGINT", "VARCHAR(10)"), Collections.singletonList(0)));
    }

    @Test
    @DisplayName("supportsGroupKeyColumns: composite key passes")
    void compositeKeySupported() {
        assertTrue(ValidateChangelogNormalizeOPStrategy.supportsGroupKeyColumns(
                Arrays.asList("BIGINT", "INTEGER", "VARCHAR(10)"), Arrays.asList(0, 1)));
    }

    @Test
    @DisplayName("supportsGroupKeyColumns: all whitelisted key types pass (mirrors E2E doc 06)")
    void allWhitelistedTypesSupported() {
        List<String> types = Arrays.asList(
                "BOOLEAN", "INTEGER", "BIGINT", "VARCHAR(10)", "CHAR(1)", "DATE",
                "TIME_WITHOUT_TIME_ZONE(0)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)");
        assertTrue(ValidateChangelogNormalizeOPStrategy.supportsGroupKeyColumns(
                types, Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8)));
    }

    @Test
    @DisplayName("supportsGroupKeyColumns: NOT NULL suffix stripped")
    void notNullSuffixSupported() {
        assertTrue(ValidateChangelogNormalizeOPStrategy.supportsGroupKeyColumns(
                Arrays.asList("BIGINT NOT NULL", "VARCHAR(10)"), Collections.singletonList(0)));
    }

    @Test
    @DisplayName("supportsGroupKeyColumns: empty/null/non-Integer/out-of-range keys rejected")
    void invalidKeysRejected() {
        List<String> types = Arrays.asList("BIGINT", "BIGINT");
        assertFalse(ValidateChangelogNormalizeOPStrategy.supportsGroupKeyColumns(types, Collections.emptyList()));
        assertFalse(ValidateChangelogNormalizeOPStrategy.supportsGroupKeyColumns(types, null));
        assertFalse(ValidateChangelogNormalizeOPStrategy.supportsGroupKeyColumns(types, Collections.singletonList("0")));
        assertFalse(ValidateChangelogNormalizeOPStrategy.supportsGroupKeyColumns(types, Collections.singletonList(5)));
    }

    @Test
    @DisplayName("supportsGroupKeyColumns: DOUBLE/DECIMAL/TIME(>0)/TIMESTAMP(>3) keys rejected")
    void unsupportedTypesRejected() {
        assertFalse(ValidateChangelogNormalizeOPStrategy.supportsGroupKeyColumns(
                Arrays.asList("DOUBLE", "BIGINT"), Collections.singletonList(0)));
        assertFalse(ValidateChangelogNormalizeOPStrategy.supportsGroupKeyColumns(
                Arrays.asList("DECIMAL64(10,2)", "BIGINT"), Collections.singletonList(0)));
        assertFalse(ValidateChangelogNormalizeOPStrategy.supportsGroupKeyColumns(
                Arrays.asList("TIME_WITHOUT_TIME_ZONE(1)", "BIGINT"), Collections.singletonList(0)));
        assertFalse(ValidateChangelogNormalizeOPStrategy.supportsGroupKeyColumns(
                Arrays.asList("TIMESTAMP_WITHOUT_TIME_ZONE(9)", "BIGINT"), Collections.singletonList(0)));
    }

    @Test
    @DisplayName("executeValidateOperator: single-key description passes")
    void validSingleKeyDescription() {
        assertTrue(new ValidateChangelogNormalizeOPStrategy().executeValidateOperator(validDescription()));
    }

    @Test
    @DisplayName("executeValidateOperator: composite-key description passes")
    void validCompositeKeyDescription() {
        Map<String, Object> m = validDescription();
        m.put("inputTypes", Arrays.asList("BIGINT", "INTEGER", "VARCHAR(10)"));
        m.put("grouping", Arrays.asList(0, 1));
        assertTrue(new ValidateChangelogNormalizeOPStrategy().executeValidateOperator(m));
    }

    @Test
    @DisplayName("executeValidateOperator: unsupported non-key column rejected")
    void unsupportedNonKeyTypeRejected() {
        Map<String, Object> m = validDescription();
        m.put("inputTypes", Arrays.asList("BIGINT", "DOUBLE"));
        assertFalse(new ValidateChangelogNormalizeOPStrategy().executeValidateOperator(m));
    }

    @Test
    @DisplayName("executeValidateOperator: miniBatch/TTL/inputInsertOnly/wrong function/empty grouping rejected")
    void contractFlagsRejected() {
        Map<String, Object> miniBatch = validDescription();
        miniBatch.put("miniBatchEnabled", Boolean.TRUE);
        assertFalse(new ValidateChangelogNormalizeOPStrategy().executeValidateOperator(miniBatch));

        Map<String, Object> ttl = validDescription();
        ttl.put("stateRetentionTime", 5000L);
        assertFalse(new ValidateChangelogNormalizeOPStrategy().executeValidateOperator(ttl));

        Map<String, Object> insertOnly = validDescription();
        insertOnly.put("inputInsertOnly", Boolean.TRUE);
        assertFalse(new ValidateChangelogNormalizeOPStrategy().executeValidateOperator(insertOnly));

        Map<String, Object> wrongFn = validDescription();
        wrongFn.put("processFunction", "OtherFunction");
        assertFalse(new ValidateChangelogNormalizeOPStrategy().executeValidateOperator(wrongFn));

        Map<String, Object> emptyGrouping = validDescription();
        emptyGrouping.put("grouping", Collections.emptyList());
        assertFalse(new ValidateChangelogNormalizeOPStrategy().executeValidateOperator(emptyGrouping));
    }
}
