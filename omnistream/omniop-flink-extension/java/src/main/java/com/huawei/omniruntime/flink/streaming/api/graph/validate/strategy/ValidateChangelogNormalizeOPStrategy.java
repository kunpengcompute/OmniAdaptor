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
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ValidateChangelogNormalizeOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Set<String> SUPPORT_PROCESS_FUNCTION_NAMES =
            Collections.singleton("ProcTimeDeduplicateKeepLastRowFunction");

    private static final List<String> SUPPORT_GROUP_KEY_TYPES =
            Arrays.asList("BIGINT", "INTEGER", "STRING", "VARCHAR");

    private static final List<Number> SUPPORT_STATE_RETENTION_TIMES = Arrays.asList(0, 0L);
    private static final Boolean SUPPORT_MINI_BATCH_ENABLED = Boolean.FALSE;

    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        if (!SUPPORT_PROCESS_FUNCTION_NAMES.contains(operatorInfoMap.get("processFunction"))
                || !SUPPORT_MINI_BATCH_ENABLED.equals(operatorInfoMap.get("miniBatchEnabled"))) {
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
        return keys.stream().allMatch(index -> index instanceof Integer && supportsPartitionKey((Integer) index, types));
    }

    private static boolean supportsPartitionKey(int index, List<?> types) {
        if (index < 0 || index >= types.size() || !(types.get(index) instanceof String)) {
            return false;
        }
        String type = stripNotNull((String) types.get(index));
        return SUPPORT_GROUP_KEY_TYPES.contains(type.startsWith("VARCHAR(") ? "VARCHAR" : type);
    }
}
