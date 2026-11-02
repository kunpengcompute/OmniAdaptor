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

import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * GroupTableAggregate 的点名白名单校验（参照 count_char / group.sql 的 UDF 点名适配模式）。
 *
 * <p>单一事实源在 StreamExecGroupTableAggregate#isNativeTop2Plan：只有"单个 Top2 表聚合 UDF
 * + 单个 BIGINT 参数列"的计划才会往 description JSON 里写 processFunction 字段；其他任何
 * UDF 的 JSON 没有该字段。因此本策略只需信任该字段并做类型白名单校验——两层判定天然一致，
 * 不会出现"校验放行但 native 端不认识"的裂缝。
 */
public class ValidateGroupTableAggOPStrategy extends AbstractValidateOperatorStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateGroupTableAggOPStrategy.class);

    /** Native dispatch key written by StreamExecGroupTableAggregate for the whitelisted Top2 UDF. */
    private static final String NATIVE_PROCESS_FUNCTION = "GroupTableAggregateFunction";

    /** v1 native scope: BIGINT grouping key (category-style keys, matching the Rank strategy). */
    private static final List<String> SUPPORT_GROUP_KEY_TYPES = Collections.singletonList("BIGINT");

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        String processFunction = getStringInfo(operatorInfoMap, "processFunction");
        if (!NATIVE_PROCESS_FUNCTION.equals(processFunction)) {
            // Not the whitelisted Top2 UDF: keep the plain Java GroupTableAggFunction operator.
            LOG.info(
                    "GroupTableAggregate falls back to Java: plan has no native processFunction "
                            + "(aggregate UDF is not the whitelisted Top2)");
            return false;
        }

        List<String> inputTypeList = (ArrayList<String>) operatorInfoMap.get("inputTypes");
        if (CollectionUtil.isNullOrEmpty(inputTypeList)) {
            LOG.info("GroupTableAggregate validate failed: inputTypes is null or empty");
            return false;
        }

        Object groupingObj = operatorInfoMap.get("grouping");
        if (!(groupingObj instanceof List)) {
            LOG.info("GroupTableAggregate validate failed: grouping is missing");
            return false;
        }
        List<Integer> grouping = (List<Integer>) groupingObj;
        if (CollectionUtil.isNullOrEmpty(grouping)) {
            // Global table aggregate without grouping keys is not a keyed plan: fall back.
            LOG.info("GroupTableAggregate validate failed: no grouping keys");
            return false;
        }
        for (int key : grouping) {
            String keyType = stripNotNull(inputTypeList.get(key));
            if (!SUPPORT_GROUP_KEY_TYPES.contains(keyType)) {
                LOG.info("GroupTableAggregate validate failed: group key type {} is not supported", keyType);
                return false;
            }
        }

        // Common type whitelist over the full input/output schemas.
        List<List<String>> dataTypesList = new ArrayList<>(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
        return validateDataTypes(dataTypesList);
    }
}
