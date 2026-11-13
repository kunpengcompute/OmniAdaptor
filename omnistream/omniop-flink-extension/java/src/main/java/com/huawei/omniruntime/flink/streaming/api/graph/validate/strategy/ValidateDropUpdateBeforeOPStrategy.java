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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Validate strategy for the {@code DropUpdateBefore} operator.
 *
 * <p>The DropUpdateBefore operator is a stateless row-kind filter that only removes
 * {@code UPDATE_BEFORE} messages. It never interprets column values, so validation only checks the
 * {@code operator} marker injected by the shadowed planner node and that every field type declared
 * in {@code inputTypes}/{@code outputTypes} is supported by the native layer.
 */
public class ValidateDropUpdateBeforeOPStrategy extends AbstractValidateOperatorStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateDropUpdateBeforeOPStrategy.class);

    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        if (!"DropUpdateBefore".equals(operatorInfoMap.get("operator"))) {
            LOG.info("Missing operator marker 'DropUpdateBefore'.");
            return false;
        }
        if (!operatorInfoMap.containsKey("inputTypes") || !operatorInfoMap.containsKey("outputTypes")) {
            LOG.info("Missing inputTypes/outputTypes fields.");
            return false;
        }
        List<List<String>> dataTypes = getDataTypes(operatorInfoMap, "inputTypes", "outputTypes");
        return validateDataTypes(dataTypes);
    }
}
