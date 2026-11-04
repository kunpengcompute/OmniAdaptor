/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
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
 * Validate strategy for the SinkUpsertMaterializer
 * (org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer,
 * EXPLAIN name "SinkMaterializer(pk=[...])").
 *
 * <p>CommonExecSink plans the materializer between a retract-changelog input and
 * an upsert sink (table.exec.sink.upsert-materialize). The shadowed
 * CommonExecSink overlay writes the dispatch contract as the transformation
 * description JSON: "grouping" (sink primary key indices, also the keyed state
 * key), optional "inputUpsertKey" (upstream upsert key indices, empty/null
 * means whole-row comparison), and inputTypes/outputTypes (identical row
 * schema: the materializer only rewrites RowKind).
 *
 * <p>The native operator is a KeyedProcessOperator keyed by the sink primary
 * key with a per-key row list state, so only the JSON contract and the row
 * types need to be checked here: "grouping" must be a non-empty list of
 * in-range column indices, "inputUpsertKey" must be null or a list of in-range
 * column indices, and inputTypes/outputTypes must fit the native compare and
 * output copy paths (INT/LONG/DOUBLE/BOOLEAN/CHAR/VARCHAR plus TIMESTAMP(0-3)
 * only - DECIMAL/TIME/DATE/TIMESTAMP_LTZ fall back to vanilla Flink).
 */
public class ValidateSinkMaterializerOPStrategy extends AbstractValidateOperatorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateSinkMaterializerOPStrategy.class);

    // Materializer-specific type whitelist aligned with the native column
    // compare/copy switch (SinkUpsertMaterializer::rowsEqual/copyRowToOutput):
    // INT, LONG, DOUBLE, BOOLEAN, CHAR, VARCHAR/STRING and TIMESTAMP(0-3).
    // DECIMAL/TIME/DATE/TIMESTAMP_LTZ and other types fall back to vanilla Flink.
    private static final Set<String> SUPPORT_MATERIALIZER_TYPES = new HashSet<>(Arrays.asList(
            "BIGINT",
            "INTEGER",
            "DOUBLE",
            "BOOLEAN",
            "CHAR",
            "VARCHAR",
            "STRING",
            "TIMESTAMP_WITHOUT_TIME_ZONE(0)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(1)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(2)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)"));

    @Override
    public boolean executeValidateOperator(Map<String, Object> operatorInfoMap) {
        List<Integer> grouping = getIndexList(operatorInfoMap, "grouping");
        if (grouping == null || grouping.isEmpty()) {
            LOG.info("validateSinkMaterializer not support: grouping is missing or empty");
            return false;
        }
        // inputUpsertKey is optional (null or empty means the upstream has no
        // upsert key: the native operator compares the whole row).
        List<Integer> inputUpsertKey = getIndexList(operatorInfoMap, "inputUpsertKey");
        if (inputUpsertKey == null) {
            Object raw = operatorInfoMap.get("inputUpsertKey");
            if (raw != null) {
                LOG.info("validateSinkMaterializer not support: inputUpsertKey is not a list of indices");
                return false;
            }
        }

        List<String> inputTypes = getTypeList(operatorInfoMap, "inputTypes");
        List<String> outputTypes = getTypeList(operatorInfoMap, "outputTypes");
        if (inputTypes == null || outputTypes == null) {
            LOG.info("validateSinkMaterializer not support: inputTypes/outputTypes is missing");
            return false;
        }
        for (int index : grouping) {
            if (index < 0 || index >= inputTypes.size()) {
                LOG.info("validateSinkMaterializer not support: grouping index {} out of range", index);
                return false;
            }
        }
        if (inputUpsertKey != null) {
            for (int index : inputUpsertKey) {
                if (index < 0 || index >= inputTypes.size()) {
                    LOG.info("validateSinkMaterializer not support: inputUpsertKey index {} out of range", index);
                    return false;
                }
            }
        }

        List<List<String>> dataTypesList = new ArrayList<>();
        dataTypesList.add(inputTypes);
        dataTypesList.add(outputTypes);
        return validateMaterializerDataTypes(dataTypesList);
    }

    private boolean validateMaterializerDataTypes(List<List<String>> dataTypesList) {
        for (List<String> dataTypes : dataTypesList) {
            for (String type : dataTypes) {
                String normalized = stripNotNull(type);
                if (normalized.matches("^VARCHAR\\([^)]*\\)$")) {
                    normalized = "VARCHAR";
                }
                if (!SUPPORT_MATERIALIZER_TYPES.contains(normalized)) {
                    LOG.info("validateSinkMaterializer not support: data type '{}' (original: '{}')", normalized, type);
                    return false;
                }
            }
        }
        return true;
    }

    private static List<Integer> getIndexList(Map<String, Object> operatorInfoMap, String name) {
        Object value = operatorInfoMap.get(name);
        if (!(value instanceof List)) {
            return null;
        }
        List<Integer> result = new ArrayList<>();
        for (Object element : (List<?>) value) {
            if (!(element instanceof Number)) {
                LOG.info("validateSinkMaterializer not support: {} is not a list of indices", name);
                return null;
            }
            result.add(((Number) element).intValue());
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<String> getTypeList(Map<String, Object> operatorInfoMap, String name) {
        Object value = operatorInfoMap.get(name);
        if (!(value instanceof List)) {
            return null;
        }
        try {
            return (List<String>) value;
        } catch (ClassCastException exception) {
            LOG.info("validateSinkMaterializer not support: {} is not a list of strings", name);
            return null;
        }
    }
}
