/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * OmniAdaptor is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ValidateWindowAggOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateWindowAggOPStrategy.class);

    private static final Set<String> SUPPORT_AGG_FUNCTION_NAME = new HashSet<>(Arrays.asList(
            "MAX",
            "COUNT",
            "SUM",
            "MIN"));

    private static final Set<String> SUPPORT_WINDOW_TYPE = new HashSet<>(Arrays.asList("TUMBLE", "HOP"));

    private static final Set<String> SUPPORT_TIME_TYPE = new HashSet<>(Collections.singletonList("event"));

    private static final Set<String> SUPPORT_TIME_ATTRIBUTE_TYPE = new HashSet<>(Arrays.asList("TIMESTAMP_WITHOUT_TIME_ZONE(3)"));

    private static final Map<String, List<String>> SUPPORT_AGG_FUNCTION_DATATYPE = new HashMap<>();

    static {
        SUPPORT_AGG_FUNCTION_DATATYPE.put("MAX", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("COUNT", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("SUM", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("MIN", Collections.singletonList("BIGINT"));
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        List<String> inputTypeList = (ArrayList<String>) operatorInfoMap.get("inputTypes");
        // Validate SUPPORT_WINDOW_TYPE
        String windowInfo;
        Object windowInfoObj = operatorInfoMap.get("window");
        if (windowInfoObj instanceof String) {
            windowInfo = (String) operatorInfoMap.get("window");
        } else {
            return false;
        }
        String windowType = windowInfo.substring(0, windowInfo.indexOf("("));
        if (!SUPPORT_WINDOW_TYPE.contains(windowType)) {
            LOG.warn("The window type {} not support.", windowType);
            return false;
        }

        String timeType = getStringInfo(operatorInfoMap, "timeType");
        if (!SUPPORT_TIME_TYPE.contains(timeType)) {
            LOG.warn("The time type {} is not supported.", timeType);
            return false;
        }

        String timeAttributeType = (String) operatorInfoMap.get("timeAttributeType");
        if (!SUPPORT_TIME_ATTRIBUTE_TYPE.contains(timeAttributeType)) {
            LOG.warn("The time attribute type {} not support.", timeAttributeType);
            return false;
        }

        // Validate agg function. localAggregateCalls and globalAggregateCalls not Validated
        Map<String, Object> aggInfoListMap = (Map<String, Object>) operatorInfoMap.get("aggInfoList");
        List<Map<String, Object>> aggregateCalls = (ArrayList<Map<String, Object>>) aggInfoListMap.get("aggregateCalls");
        boolean inputTypesEmpty = CollectionUtil.isNullOrEmpty(inputTypeList);
        if (aggregateCalls != null) {
            for (Map<String, Object> aggregateCallMap : aggregateCalls) {
                String name = aggregateCallMap.get("name").toString();
                String functionName = name.substring(0, name.indexOf("("));
                if (!SUPPORT_AGG_FUNCTION_NAME.contains(functionName)) {
                    LOG.info("ValidateWindowAggOPStrategy not support aggCall is {}", name);
                    return false;
                }

                int filterArg = (int) aggregateCallMap.get("filterArg");
                if (filterArg != -1) {
                    LOG.warn("The aggregate function {} not support filterArg.", functionName);
                    return false;
                }

                List<Integer> argIndexes = (ArrayList<Integer>) aggregateCallMap.get("argIndexes");
                if (inputTypesEmpty || CollectionUtil.isNullOrEmpty(argIndexes)) {
                    continue;
                }
                int argIndex = argIndexes.get(0);
                String argType = stripNotNull(inputTypeList.get(argIndex));
                List<String> supportDataTypes = SUPPORT_AGG_FUNCTION_DATATYPE.get(functionName);
                if (!supportDataTypes.contains(argType)) {
                    return false;
                }
            }
        }

        // if function support,then validate dataTypes
        List<List<String>> dataTypesList = new ArrayList<>(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
        return validateDataTypes(dataTypesList);
    }
}
