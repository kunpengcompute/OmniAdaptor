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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ValidateCorrelateOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateCorrelateOPStrategy.class);
    private static final Set<String> SUPPORT_JOIN_TYPE = new HashSet<>(Arrays.asList("InnerJoin", "LeftOuterJoin"));
    private static final Set<String> NATIVE_SUPPORTED_FUNCTIONS =
            new HashSet<>(Arrays.asList("jsontest", "nrejsontest"));
    private static final String NATIVE_SUPPORTED_FUNCTION_CLASS = "org.example.sql.JsonSplit";

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        // 校验 joinType
        Object joinTypeObj = operatorInfoMap.get("joinType");
        if (joinTypeObj == null || !SUPPORT_JOIN_TYPE.contains(joinTypeObj.toString())) {
            LOG.info("Correlate unsupported joinType: {}", joinTypeObj);
            return false;
        }

        // 校验必要字段存在
        if (!operatorInfoMap.containsKey("functionName")
                || !operatorInfoMap.containsKey("functionClass")) {
            LOG.info("Correlate missing functionName or functionClass");
            return false;
        }

        String functionName = operatorInfoMap.get("functionName").toString();
        if (!NATIVE_SUPPORTED_FUNCTIONS.contains(functionName)) {
            LOG.info("Correlate function {} not natively supported, fallback", functionName);
            return false;
        }
        String functionClass = operatorInfoMap.get("functionClass").toString();
        if (!NATIVE_SUPPORTED_FUNCTION_CLASS.equals(functionClass)) {
            LOG.info("Correlate function class {} not natively supported, fallback", functionClass);
            return false;
        }

        if (!operatorInfoMap.containsKey("inputTypes") || !operatorInfoMap.containsKey("outputTypes")) {
            LOG.info("Missing inputTypes or outputTypes for Correlate operator.");
            return false;
        }

        // 校验数据类型
        return validateDataTypes(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
    }
}
