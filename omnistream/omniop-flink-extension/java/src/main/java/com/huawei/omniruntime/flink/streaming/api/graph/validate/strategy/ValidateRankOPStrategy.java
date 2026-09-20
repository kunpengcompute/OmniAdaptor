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

public class ValidateRankOPStrategy extends AbstractValidateOperatorStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateRankOPStrategy.class);

    private static final List<String> SUPPORT_GROUP_KEY_TYPES = Collections.singletonList("BIGINT");
    private static final List<String> SUPPORT_ORDER_BY_TYPES = Arrays.asList("BIGINT",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)");
    private static final List<String> SUPPORT_FUNCTIONS =
            Arrays.asList("AppendOnlyTopNFunction", "FastTop1Function", "AppendOnlyFirstNFunction");

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        List<String> inputTypeList = (ArrayList<String>) operatorInfoMap.get("inputTypes");
        List<Integer> partitionKeys = (ArrayList<Integer>) operatorInfoMap.get("partitionKey");

        Object rankFunctionObj = operatorInfoMap.get("processFunction");
        String rankFunction = null;
        if (rankFunctionObj instanceof String) {
            rankFunction = (String) rankFunctionObj;
        } else {
            return false;
        }
        List<Boolean> sortAscendingOrders = (ArrayList<Boolean>) operatorInfoMap.get("sortAscendingOrders");
        List<Integer> sortFieldIds = (ArrayList<Integer>) operatorInfoMap.get("sortFieldIndices");

        // Rank 子算子: FirstN / Top1 / TopN
        if (!SUPPORT_FUNCTIONS.contains(rankFunction)) {
            return false;
        }

        if (CollectionUtil.isNullOrEmpty(partitionKeys) || CollectionUtil.isNullOrEmpty(inputTypeList)) {
            LOG.info("failed inside isNull or empty check");
            return false;
        }

        if (partitionKeys.size() > 1) {
            return false;
        }

        for (int partitionKey : partitionKeys) {
            String keyType = stripNotNull(inputTypeList.get(partitionKey));
            if (!SUPPORT_GROUP_KEY_TYPES.contains(keyType)) {
                LOG.info("failed inside group key type check {} ", keyType);
                return false;
            }
        }

        if (!validateTop1Function(operatorInfoMap, inputTypeList, sortFieldIds)) {
            return false;
        }

        if (!validateTopNFunction(operatorInfoMap, inputTypeList, sortFieldIds, sortAscendingOrders)) {
            return false;
        }

        if (!validateFirstNFunction(operatorInfoMap, inputTypeList, sortFieldIds, sortAscendingOrders)) {
            return false;
        }

        // if function support,then validate dataTypes
        List<List<String>> dataTypesList = new ArrayList<>(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
        LOG.info("Otherwise failed for dataTypes");
        return validateDataTypes(dataTypesList);
    }

    private boolean validateTop1Function(Map<String, Object> operatorInfoMap, List<String> inputTypeList,
                                         List<Integer> sortFieldIds) {
        // TOP1
        if (!operatorInfoMap.get("processFunction").equals("FastTop1Function")) {
            return true;
        }

        if (sortFieldIds.size() > 2) {
            return false;
        }
        for (int sortId : sortFieldIds) {
            if (!SUPPORT_ORDER_BY_TYPES.contains(stripNotNull(inputTypeList.get(sortId)))) {
                return false;
            }
        }

        return true;
    }

    private boolean validateTopNFunction(Map<String, Object> operatorInfoMap, List<String> inputTypeList,
                                         List<Integer> sortFieldIds, List<Boolean> sortAscendingOrders) {
        // TOP N  more strict condition
        if (!operatorInfoMap.get("processFunction").equals("AppendOnlyTopNFunction")) {
            return true;
        }

        return sortFieldIds.size() <= 1 && stripNotNull(inputTypeList.get(sortFieldIds.get(0))).equals("BIGINT")
                && !sortAscendingOrders.get(0);
    }

    private boolean validateFirstNFunction(Map<String, Object> operatorInfoMap, List<String> inputTypeList,
                                           List<Integer> sortFieldIds, List<Boolean> sortAscendingOrders) {
        if (!operatorInfoMap.get("processFunction").equals("AppendOnlyFirstNFunction")) {
            return true;
        }
        Object rankRangeObj = operatorInfoMap.get("rankRange");
        if (!(rankRangeObj instanceof String)) {
            LOG.info("validate AppendOnlyFirstNFunction missing constant rankRange");
            return false;
        }
        String rankRange = (String) rankRangeObj;
        if (!rankRange.matches(".*rankStart=\\d+.*rankEnd=\\d+.*")) {
            LOG.info("validate AppendOnlyFirstNFunction only supports constant rankRange, got {}", rankRange);
            return false;
        }
        if (CollectionUtil.isNullOrEmpty(sortFieldIds) || CollectionUtil.isNullOrEmpty(sortAscendingOrders)
                || sortFieldIds.size() != 1 || sortAscendingOrders.size() < 1) {
            LOG.info("validate AppendOnlyFirstNFunction requires a single sort field");
            return false;
        }
        if (!Boolean.TRUE.equals(sortAscendingOrders.get(0))) {
            LOG.info("validate AppendOnlyFirstNFunction requires ascending proctime order");
            return false;
        }
        int sortId = sortFieldIds.get(0);
        if (sortId < 0 || sortId >= inputTypeList.size()) {
            LOG.info("validate AppendOnlyFirstNFunction sort index {} out of bound", sortId);
            return false;
        }
        String sortType = stripNotNull(inputTypeList.get(sortId));
        if (sortType == null || !sortType.startsWith("TIMESTAMP_WITH_LOCAL_TIME_ZONE")) {
            LOG.info("validate AppendOnlyFirstNFunction sort type {} is not proctime", sortType);
            return false;
        }
        return true;
    }
}
