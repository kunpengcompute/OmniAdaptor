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

import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ValidateDeduplicateOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateDeduplicateOPStrategy.class);

    private static final List<String> SUPPORT_GROUP_KEY_TYPES = Collections.singletonList("BIGINT");

    // Jackson 把 JSON 整数读成 Integer 或 Long。mini-batch 关闭时 miniBatchSize 为 -1；
    // 字段缺失按未开启处理。状态 TTL 必须显式为 0，原生 seen-flag / Keep-Last 都不会过期。
    private static final List<Number> DISABLED_MINI_BATCH_SIZES = Arrays.asList(-1, -1L);
    private static final List<Number> DISABLED_RETENTION_TIMES = Arrays.asList(0, 0L);

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        boolean isRowtime = (boolean) operatorInfoMap.get("isRowtime");
        // ProcTime Keep-First / Keep-Last 都已有原生实现。mini-batch 会换成
        // KeyedMapBundleOperator，TTL 会在过期后重发同一 key，这两条都没有原生语义。
        if (!isRowtime && !validateProcTimeShape(operatorInfoMap)) {
            return false;
        }
        List<String> inputTypeList = (ArrayList<String>) operatorInfoMap.get("inputTypes");
        List<Integer> uniqueKeys = (ArrayList<Integer>) operatorInfoMap.get("grouping");
        if (!CollectionUtil.isNullOrEmpty(uniqueKeys) && !CollectionUtil.isNullOrEmpty(inputTypeList)) {
            for (int uniqueKey : uniqueKeys) {
                String keyType = stripNotNull(inputTypeList.get(uniqueKey));
                if (!SUPPORT_GROUP_KEY_TYPES.contains(keyType)) {
                    return false;
                }
            }
        }
        // if function support,then validate dataTypes
        List<List<String>> dataTypesList = new ArrayList<>(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
        return validateDataTypes(dataTypesList);
    }

    private static boolean validateProcTimeShape(Map<String, Object> operatorInfoMap) {
        if (operatorInfoMap.get("keepLastRow") == null) {
            LOG.info("validateVertexChainInfoForOmniTask ProcTime Deduplicate missing keepLastRow");
            return false;
        }
        Object miniBatchSize = operatorInfoMap.get("miniBatchSize");
        if (miniBatchSize != null && !DISABLED_MINI_BATCH_SIZES.contains(miniBatchSize)) {
            LOG.info("validateVertexChainInfoForOmniTask ProcTime Deduplicate does not support mini-batch, "
                    + "miniBatchSize={}", miniBatchSize);
            return false;
        }
        Object minRetentionTime = operatorInfoMap.get("minRetentionTime");
        if (!DISABLED_RETENTION_TIMES.contains(minRetentionTime)) {
            LOG.info("validateVertexChainInfoForOmniTask ProcTime Deduplicate requires minRetentionTime=0, "
                    + "minRetentionTime={}", minRetentionTime);
            return false;
        }
        return true;
    }
}
