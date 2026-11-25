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

import java.util.*;

/**
 * the strategy for GroupWindowAggregate operator.
 *
 * @since 2025-05-30
 */
public class ValidateGroupWindowAggOPStrategy extends AbstractValidateOperatorStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateGroupWindowAggOPStrategy.class);

    private static final Set<String> SUPPORT_AGG_FUNCTION_NAME = new HashSet<>(Arrays.asList("COUNT", "SUM", "MAX", "MIN"));

    private static final Set<String> SUPPORT_WINDOW_TYPE = new HashSet<>(
        Arrays.asList("SessionGroupWindow", "TumblingGroupWindow", "SlidingGroupWindow"));

    private static final Set<String> SUPPORT_TIME_TYPE = new HashSet<>(
        Collections.singletonList("event"));

    private static final Set<String> SUPPORT_TIME_ATTRIBUTE_TYPE = new HashSet<>(Arrays.asList("TIMESTAMP_WITHOUT_TIME_ZONE(3)"));

    private static final Map<String, List<String>> SUPPORT_AGG_FUNCTION_DATATYPE = new HashMap<>();

    static {
        SUPPORT_AGG_FUNCTION_DATATYPE.put("COUNT", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("SUM", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("MAX", Collections.singletonList("BIGINT"));
        SUPPORT_AGG_FUNCTION_DATATYPE.put("MIN", Collections.singletonList("BIGINT"));
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        List<String> inputTypeList = (ArrayList<String>) operatorInfoMap.get("inputTypes");
        // Validate SUPPORT_WINDOW_TYPE
        String windowInfo = getStringInfo(operatorInfoMap, "windowType");
        if (windowInfo == null) {
            LOG.warn("The windowType field is null.");
            return false;
        }
        String windowType = windowInfo.substring(0, windowInfo.indexOf("("));
        if (!SUPPORT_WINDOW_TYPE.contains(windowType)) {
            LOG.warn("The window type {} is not supported.", windowType);
            return false;
        }

        // Validate SUPPORT_TIME_TYPE
        String timeType = getStringInfo(operatorInfoMap, "timeType");
        if (!SUPPORT_TIME_TYPE.contains(timeType)) {
            LOG.warn("The time type {} is not supported.", timeType);
            return false;
        }

        // Validate SUPPORT_TIME_ATTRIBUTE_TYPE
        String timeAttributeType = getStringInfo(operatorInfoMap, "timeAttributeType");
        if (!SUPPORT_TIME_ATTRIBUTE_TYPE.contains(timeAttributeType)) {
            LOG.warn("The time attribute type {} is not supported.", timeAttributeType);
            return false;
        }

        // Validate agg function
        Map<String, Object> aggInfoListMap = (Map<String, Object>) operatorInfoMap.get("aggInfoList");
        List<Map<String, Object>> aggregateCalls =
            (ArrayList<Map<String, Object>>) aggInfoListMap.get("aggregateCalls");
        boolean inputTypesEmpty = CollectionUtil.isNullOrEmpty(inputTypeList);
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
                // when argIndexes is empty, the agg function may be COUNT(*)
                continue;
            }
            if (argIndexes.size() > 1) {
                LOG.warn("The aggregate function {} not support more than one argument.", functionName);
                return false;
            }
            int argIndex = argIndexes.get(0);
            String argType = stripNotNull(inputTypeList.get(argIndex));
            List<String> supportDataTypes = SUPPORT_AGG_FUNCTION_DATATYPE.get(functionName);
            if (!supportDataTypes.contains(argType)) {
                LOG.info("The aggregate data type {} is not supported in aggregate function {}.", argType, functionName);
                return false;
            }
        }

        // if function support,then validate dataTypes
        List<List<String>> dataTypesList = new ArrayList<>(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
        return validateDataTypes(dataTypesList);
    }
}
