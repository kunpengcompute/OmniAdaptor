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
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ValidateDeduplicateOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateDeduplicateOPStrategy.class);

    private static final List<String> SUPPORT_GROUP_KEY_TYPES = Collections.singletonList("BIGINT");

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        boolean isRowtime = (boolean) operatorInfoMap.get("isRowtime");
        // not support ProcTimeDeduplicateOperator , just support RowtimeDeduplicateOperator
        if (!isRowtime) {
            LOG.info("validateVertexChainInfoForOmniTask not support isRowtime is {}", isRowtime);
            return false;
        }
        List<String> inputTypeList = (ArrayList<String>) operatorInfoMap.get("inputTypes");
        List<Integer> uniqueKeys = (ArrayList<Integer>) operatorInfoMap.get("grouping");
        if (!CollectionUtil.isNullOrEmpty(uniqueKeys) && !CollectionUtil.isNullOrEmpty(inputTypeList)) {
            for (int uniqueKey : uniqueKeys) {
                String keyType = inputTypeList.get(uniqueKey);
                if (!SUPPORT_GROUP_KEY_TYPES.contains(keyType)) {
                    return false;
                }
            }
        }
        // if function support,then validate dataTypes
        List<List<String>> dataTypesList = new ArrayList<>(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
        return validateDataTypes(dataTypesList);
    }
}
