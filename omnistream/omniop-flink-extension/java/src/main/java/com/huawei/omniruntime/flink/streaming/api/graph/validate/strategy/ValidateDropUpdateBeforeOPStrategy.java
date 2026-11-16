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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private static final Set<String> EXTRA_SUPPORT_DATA_TYPE =
            new HashSet<>(Arrays.asList("DATE"));

    @Override
    public boolean validateDataTypes(List<List<String>> dataTypesList) {
        return dataTypesList.stream()
                .flatMap(List::stream)
                .allMatch(type -> {
                    String originalType = type;
                    type = stripNotNull(type);

                    if (type.matches("^VARCHAR\\([^)]*\\)$")) {
                        LOG.info("converted to VARCHAR");
                        type = "VARCHAR";
                    }

                    if (type.matches("^CHAR\\([^)]*\\)$")) {
                        LOG.info("converted to CHAR");
                        type = "CHAR";
                    }

                    if (type.matches("^DECIMAL64\\([^)]*\\)$")) {
                        LOG.info("converted to DECIMAL64");
                        type = "DECIMAL64";
                    }

                    if (type.matches("^DECIMAL128\\([^)]*\\)$")) {
                        LOG.info("converted to DECIMAL128");
                        type = "DECIMAL128";
                    }

                    if (!SUPPORT_DATA_TYPE.contains(type) && !EXTRA_SUPPORT_DATA_TYPE.contains(type)) {
                        LOG.info("Unsupported data type: '{}' (original: '{}')", type, originalType);
                        return false;
                    }
                    return true;
                });
    }

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
