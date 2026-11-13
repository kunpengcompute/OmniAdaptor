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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ValidateChangelogNormalizeOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Set<String> SUPPORT_PROCESS_FUNCTION_NAMES =
            Collections.singleton("ProcTimeDeduplicateKeepLastRowFunction");

    private static final List<String> SUPPORT_GROUP_KEY_TYPES =
            Arrays.asList(
                    "BIGINT",
                    "INTEGER",
                    "STRING",
                    "VARCHAR",
                    "CHAR",
                    "BOOLEAN",
                    "DATE",
                    "TIME",
                    "TIME_WITHOUT_TIME_ZONE",
                    "TIMESTAMP",
                    "TIMESTAMP_WITHOUT_TIME_ZONE",
                    "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
                    "TIMESTAMP_LTZ");

    // 非键列支持与分组键列一致的类型。原生 ProcTimeDeduplicateKeepLastRowFunction 的
    // 输入抽取（VectorBatch::extractRowData）与输出转换均把 TIME / TIMESTAMP_LTZ 变体
    // 按 OMNI_LONG（int64 毫秒）处理，因此非键列无需单独剔除 TIME / TIMESTAMP_LTZ。
    private static final List<Number> SUPPORT_STATE_RETENTION_TIMES = Arrays.asList(0, 0L);
    private static final Boolean SUPPORT_MINI_BATCH_ENABLED = Boolean.FALSE;

    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        if (!SUPPORT_PROCESS_FUNCTION_NAMES.contains(operatorInfoMap.get("processFunction"))
                || !SUPPORT_MINI_BATCH_ENABLED.equals(operatorInfoMap.get("miniBatchEnabled"))) {
            return false;
        }
        // ChangelogNormalize 契约：inputInsertOnly 必须为 false（Flink
        // StreamExecChangelogNormalize 以 inputInsertOnly=false 构造同一函数）。
        // 缺失（旧计划/历史描述）按 false 处理以保持兼容。
        Object inputInsertOnly = operatorInfoMap.get("inputInsertOnly");
        if (inputInsertOnly != null && !Boolean.FALSE.equals(inputInsertOnly)) {
            return false;
        }
        Object stateRetentionTime = operatorInfoMap.get("stateRetentionTime");
        if (!SUPPORT_STATE_RETENTION_TIMES.contains(stateRetentionTime)) {
            return false;
        }
        Object inputTypes = operatorInfoMap.get("inputTypes");
        Object grouping = operatorInfoMap.get("grouping");
        if (!(inputTypes instanceof List) || !(grouping instanceof List)) {
            return false;
        }
        List<?> types = (List<?>) inputTypes;
        List<?> keys = (List<?>) grouping;
        if (keys.isEmpty()) {
            return false;
        }
        if (!keys.stream().allMatch(index -> index instanceof Integer && supportsPartitionKey((Integer) index, types))) {
            return false;
        }
        Set<Integer> keyIndexes = new HashSet<>();
        for (Object key : keys) {
            keyIndexes.add((Integer) key);
        }
        for (int i = 0; i < types.size(); ++i) {
            if (!keyIndexes.contains(i) && !supportsNonKeyColumn(i, types)) {
                return false;
            }
        }
        return true;
    }

    private static boolean supportsPartitionKey(int index, List<?> types) {
        return supportsColumnType(index, types);
    }

    private static boolean supportsNonKeyColumn(int index, List<?> types) {
        return supportsColumnType(index, types);
    }

    private static boolean supportsColumnType(int index, List<?> types) {
        if (index < 0 || index >= types.size() || !(types.get(index) instanceof String)) {
            return false;
        }
        return isSupportedGroupKeyType((String) types.get(index));
    }

    /**
     * 校验单个分组键（主键）列类型：类型基名须在 {@link #SUPPORT_GROUP_KEY_TYPES} 白名单内，
     * 且时间类型精度符合 native 约束（TIME 仅精度 0，TIMESTAMP / TIMESTAMP_LTZ <= 3）。
     *
     * <p>该方法是 ChangelogNormalize 分组键类型规则的唯一来源，供本策略的分组键/非键列校验，
     * 以及 upsert-kafka 源复合主键校验（{@code OmniGraphOverride}）共用，避免规则重复。
     */
    public static boolean isSupportedGroupKeyType(String flinkType) {
        String type = stripNotNull(flinkType);
        return type != null
                && SUPPORT_GROUP_KEY_TYPES.contains(normalizeGroupKeyType(type))
                && supportsKeyPrecision(type);
    }

    /**
     * 校验一组分组键（主键）列索引（复合键）：索引非空、均为合法下标，且每列类型受支持。
     *
     * <p>upsert-kafka 源的复合主键能否走 native 由本方法决定，从而把 ChangelogNormalize 的
     * 复合键语义集中在本策略维护，而不是分散到 {@code OmniGraphOverride}。
     */
    public static boolean supportsGroupKeyColumns(List<?> types, List<?> keyIndexes) {
        if (keyIndexes == null || keyIndexes.isEmpty()) {
            return false;
        }
        for (Object key : keyIndexes) {
            if (!(key instanceof Integer)) {
                return false;
            }
            int index = (Integer) key;
            if (index < 0 || index >= types.size() || !(types.get(index) instanceof String)) {
                return false;
            }
            if (!isSupportedGroupKeyType((String) types.get(index))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Strips the parenthesized precision/length suffix so that "VARCHAR(10)", "CHAR(10)",
     * "TIME_WITHOUT_TIME_ZONE(0)" and "TIMESTAMP_WITHOUT_TIME_ZONE(3)" match their base names.
     */
    private static String normalizeGroupKeyType(String type) {
        int openParen = type.indexOf('(');
        if (openParen >= 0) {
            return type.substring(0, openParen);
        }
        return type;
    }

    /**
     * native 时间类型是 int64 毫秒。TIME（TIME / TIME_WITHOUT_TIME_ZONE）只允许精度 0
     * 走原生，TIME(1..3) 回退 Flink；TIMESTAMP 保持原行为（精度 <= 3），TIMESTAMP(9) 等
     * 纳秒精度仍被拒绝。
     */
    private static boolean supportsKeyPrecision(String type) {
        // TIMESTAMP 也以 "TIME" 开头，必须先判断 TIMESTAMP。
        boolean isTimestamp = type.startsWith("TIMESTAMP");
        boolean isTime = !isTimestamp && type.startsWith("TIME");
        if (!isTime && !isTimestamp) {
            return true;
        }
        int maxPrecision = isTime ? 0 : 3;
        int openParen = type.indexOf('(');
        if (openParen < 0) {
            return true;
        }
        int closeParen = type.indexOf(')', openParen);
        if (closeParen <= openParen + 1) {
            return false;
        }
        try {
            int precision = Integer.parseInt(type.substring(openParen + 1, closeParen));
            return precision >= 0 && precision <= maxPrecision;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
