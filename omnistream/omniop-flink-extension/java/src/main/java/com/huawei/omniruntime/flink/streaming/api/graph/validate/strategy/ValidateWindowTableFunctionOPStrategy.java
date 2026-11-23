/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT.
 * See the Mulan PSL v2 for more details.
 */

package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validate strategy for the standalone Window TVF operator ({@code WindowTableFunction}).
 *
 * <p>Supports TUMBLE/HOP/CUMULATE with event time or processing time, and the time attribute type
 * must be TIMESTAMP(3) or TIMESTAMP_LTZ(3) which the native runtime can carry as int64 millis.
 */
public class ValidateWindowTableFunctionOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateWindowTableFunctionOPStrategy.class);

    private static final Set<String> SUPPORT_WINDOW_TYPE = new HashSet<>(Arrays.asList("TUMBLE", "HOP", "CUMULATE"));

    private static final Set<String> SUPPORT_TIME_TYPE = new HashSet<>(Arrays.asList("event", "processing"));

    private static final Set<String> SUPPORT_TIME_ATTRIBUTE_TYPE = new HashSet<>(Arrays.asList(
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)"));

    // The operator only copies input columns by position and appends window columns, so it never
    // interprets column values. DATE is carried natively as OMNI_DATE32 (see LogicalType), thus it
    // can be accepted even though the shared whitelist does not include it.
    private static final Set<String> EXTRA_SUPPORT_DATA_TYPE = new HashSet<>(Arrays.asList("DATE"));

    @Override
    public boolean validateDataTypes(List<List<String>> dataTypesList) {
        return dataTypesList.stream()
                .flatMap(List::stream)
                .allMatch(type -> {
                    String originalType = type;
                    type = stripNotNull(type);

                    if (type.matches("^VARCHAR\\([^)]*\\)$")) {
                        type = "VARCHAR";
                    }

                    if (type.matches("^CHAR\\([^)]*\\)$")) {
                        type = "CHAR";
                    }

                    if (type.matches("^DECIMAL64\\([^)]*\\)$")) {
                        type = "DECIMAL64";
                    }

                    if (type.matches("^DECIMAL128\\([^)]*\\)$")) {
                        type = "DECIMAL128";
                    }

                    if (!SUPPORT_DATA_TYPE.contains(type) && !EXTRA_SUPPORT_DATA_TYPE.contains(type)) {
                        LOG.info("Unsupported data type: '{}' (original: '{}')", type, originalType);
                        return false;
                    }
                    return true;
                });
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        List<String> inputTypeList = (List<String>) operatorInfoMap.get("inputTypes");
        if (inputTypeList == null || inputTypeList.isEmpty()) {
            LOG.info("ValidateWindowTableFunctionOPStrategy missing inputTypes");
            return false;
        }

        Object windowInfoObj = operatorInfoMap.get("window");
        if (!(windowInfoObj instanceof String)) {
            return false;
        }
        String windowInfo = (String) windowInfoObj;
        int index = windowInfo.indexOf("(");
        if (index <= 0) {
            return false;
        }
        String windowType = windowInfo.substring(0, index);
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
        if (timeAttributeType == null
                || !SUPPORT_TIME_ATTRIBUTE_TYPE.contains(
                        AbstractValidateOperatorStrategy.stripNotNull(timeAttributeType))) {
            LOG.warn("The time attribute type {} is not supported.", timeAttributeType);
            return false;
        }

        if ("event".equals(timeType)) {
            Object timeAttributeIndexObj = operatorInfoMap.get("timeAttributeIndex");
            if (!(timeAttributeIndexObj instanceof Number)) {
                LOG.warn("Event time WindowTableFunction requires a valid timeAttributeIndex.");
                return false;
            }
            int timeAttributeIndex = ((Number) timeAttributeIndexObj).intValue();
            if (timeAttributeIndex < 0 || timeAttributeIndex >= inputTypeList.size()) {
                LOG.warn("The timeAttributeIndex {} is out of range of inputTypes.", timeAttributeIndex);
                return false;
            }
        }

        // Validate required window parameters per window type.
        if ("HOP".equals(windowType) && !operatorInfoMap.containsKey("windowSlide")) {
            return false;
        }
        if ("CUMULATE".equals(windowType)
                && (!operatorInfoMap.containsKey("maxSize") || !operatorInfoMap.containsKey("step"))) {
            return false;
        }

        List<List<String>> dataTypesList =
                new ArrayList<>(getDataTypes(operatorInfoMap, "inputTypes", "outputTypes"));
        return validateDataTypes(dataTypesList);
    }
}
