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
 * Validate strategy for VALUES-based sources ({@code SimpleInputFormatOperatorFactory}).
 *
 * <p>The description is injected by {@code CommonExecValues} during planning (format=values,
 * tuples, outputTypes, outputNames, batch). VALUES sources are executed natively only when the
 * description is complete and every output type is within the native VALUES whitelist; otherwise
 * they fall back to Flink.
 */
public class ValidateValuesOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateValuesOPStrategy.class);

    // 原生时间类型是 int64 毫秒，因此只能表示精度 <= 3。VALUES 源只放行白名单内的
    // 字面量类型（LogicalType.toString() 形式，含可选的 " NOT NULL" 后缀）；FLOAT、
    // INTERVAL、复杂 ROW/ARRAY/MAP、精度>3 的 TIME/TIMESTAMP、BINARY/VARBINARY 一律回退 Flink，
    // 以保证与原生 Flink 输出严格一致。
    private static final Set<String> VALUES_SUPPORT_DATA_TYPE = new HashSet<>(Arrays.asList(
            "BIGINT",
            "INT",
            "DOUBLE",
            "DATE",
            "BOOLEAN",
            "TIME(0)",
            "TIMESTAMP(0)",
            "TIMESTAMP(1)",
            "TIMESTAMP(2)",
            "TIMESTAMP(3)",
            "TIMESTAMP_LTZ(0)",
            "TIMESTAMP_LTZ(1)",
            "TIMESTAMP_LTZ(2)",
            "TIMESTAMP_LTZ(3)",
            "VARCHAR",
            "CHAR",
            "STRING",
            "DECIMAL"));

    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        if (!"values".equals(operatorInfoMap.get("format"))) {
            LOG.info("validateSource: VALUES source description missing format=values, fall back to Flink.");
            return false;
        }
        Object tuplesObj = operatorInfoMap.get("tuples");
        if (!(tuplesObj instanceof List)) {
            LOG.info("validateSource: VALUES source description missing tuples, fall back to Flink.");
            return false;
        }
        Object outputTypesObj = operatorInfoMap.get("outputTypes");
        if (!(outputTypesObj instanceof List) || ((List<?>) outputTypesObj).isEmpty()) {
            LOG.info("validateSource: VALUES source description missing outputTypes, fall back to Flink.");
            return false;
        }
        List<?> outputTypes = (List<?>) outputTypesObj;
        for (Object outputType : outputTypes) {
            if (!(outputType instanceof String) || !isValuesNativeSupportedType((String) outputType)) {
                LOG.info("validateSource: VALUES source output type '{}' is not supported for native execution, "
                        + "fall back to Flink.", outputType);
                return false;
            }
        }
        for (Object rowObj : (List<?>) tuplesObj) {
            if (!(rowObj instanceof List) || ((List<?>) rowObj).size() != outputTypes.size()) {
                LOG.info("validateSource: VALUES source tuples row width mismatch, fall back to Flink.");
                return false;
            }
        }
        LOG.info("validateSource: VALUES source is suitable for native execution.");
        return true;
    }

    private boolean isValuesNativeSupportedType(String type) {
        String stripped = AbstractValidateOperatorStrategy.stripNotNull(type);
        if (stripped == null) {
            return false;
        }
        stripped = stripped.trim();

        if (stripped.startsWith("VARCHAR(") || stripped.startsWith("CHAR(")) {
            return true;
        }
        // DECIMAL(p, s) with 1 <= p <= 38, Flink already validates the decimal range.
        if (stripped.startsWith("DECIMAL")) {
            return true;
        }
        return VALUES_SUPPORT_DATA_TYPE.contains(stripped);
    }
}
