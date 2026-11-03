/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.ValuesCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.runtime.operators.values.ValuesInputFormat;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Base {@link ExecNode} that read records from given values. */
public abstract class CommonExecValues extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    public static final String VALUES_TRANSFORMATION = "values";

    public static final String FIELD_NAME_TUPLES = "tuples";

    private static final Logger LOG = LoggerFactory.getLogger(CommonExecValues.class);

    private static final String FIELD_NAME_FORMAT = "format";

    private static final String FORMAT_VALUES = "values";

    private static final String FIELD_NAME_OUTPUT_TYPES = "outputTypes";

    private static final String FIELD_NAME_OUTPUT_NAMES = "outputNames";

    private static final String FIELD_NAME_BATCH = "batch";

    private final List<List<RexLiteral>> tuples;

    public CommonExecValues(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<List<RexLiteral>> tuples,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, Collections.emptyList(), outputType, description);
        this.tuples = tuples;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        // Replicate the base Flink CommonExecValues implementation so that the Java fallback
        // behavior is preserved, then inject the native VALUES description.
        final ValuesInputFormat inputFormat =
                ValuesCodeGenerator.generatorInputFormat(
                        config,
                        planner.getFlinkContext().getClassLoader(),
                        (RowType) getOutputType(),
                        tuples,
                        getClass().getSimpleName());
        final Transformation<RowData> transformation =
                planner.getExecEnv()
                        .createInput(inputFormat, inputFormat.getProducedType())
                        .getTransformation();
        createTransformationMeta(VALUES_TRANSFORMATION, config).fill(transformation);
        transformation.setParallelism(1);
        transformation.setMaxParallelism(1);
        try {
            transformation.setDescription(buildNativeDescription((RowType) getOutputType()));
        } catch (Exception e) {
            // Fall back to Flink runtime if the native description cannot be built.
            LOG.warn("Failed to inject VALUES native description, fall back to Flink runtime.", e);
        }
        return transformation;
    }

    /**
     * Build the native VALUES source description consumed by the C++ ValuesSourceFunction:
     * {format:"values", tuples:[["..."|null,...],...], outputTypes:[...], outputNames:[...],
     * batch:true}.
     *
     * <p>outputTypes intentionally use Flink's {@code LogicalType.toString()} (= asSummaryString)
     * summary dialect, e.g. "DECIMAL(38, 2)", "TIMESTAMP_LTZ(3)", "VARCHAR(10)", with an optional
     * " NOT NULL" suffix for non-nullable fields. The C++ side parses this via
     * LogicalType::flinkTypeToOmniTypeId (classification) and
     * LogicalType::parseDecimalPrecisionScale (decimal precision/scale); the latter only extracts
     * precision/scale from the "DECIMAL(p, s)" summary form.
     *
     * <p>Do NOT switch this list to DescriptionUtil.getFieldType(...) for consistency with other
     * operators: DescriptionUtil emits the "DECIMAL64(p,s)" / "DECIMAL128(p,s)" prefix form, which
     * the C++ parser tolerates only for type-id classification and would parse the decimal
     * precision/scale incorrectly (silent precision loss / spurious nulls in native VALUES source).
     */
    private String buildNativeDescription(RowType outputType) throws JsonProcessingException {
        Map<String, Object> description = new LinkedHashMap<>();
        description.put(FIELD_NAME_FORMAT, FORMAT_VALUES);

        List<List<String>> serializedTuples = new ArrayList<>();
        for (List<RexLiteral> row : tuples) {
            List<String> serializedRow = new ArrayList<>();
            for (RexLiteral literal : row) {
                serializedRow.add(literalToValueString(literal));
            }
            serializedTuples.add(serializedRow);
        }
        description.put(FIELD_NAME_TUPLES, serializedTuples);

        List<String> outputTypes = new ArrayList<>();
        for (RowType.RowField field : outputType.getFields()) {
            outputTypes.add(field.getType().toString());
        }
        description.put(FIELD_NAME_OUTPUT_TYPES, outputTypes);
        description.put(FIELD_NAME_OUTPUT_NAMES, outputType.getFieldNames());
        description.put(FIELD_NAME_BATCH, true);

        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        return objectMapper.writeValueAsString(description);
    }

    /**
     * Serialize one literal to its JSON value string; returns null for SQL NULL. The string form is
     * the same as the one used by Flink's JSON plan (RexNodeJsonSerializer), so the C++ side only
     * needs to parse strings (except null) per column type.
     */
    private static String literalToValueString(RexLiteral literal) {
        if (literal == null || literal.isNull()) {
            return null;
        }
        final Comparable<?> value = literal.getValueAs(Comparable.class);
        if (value == null) {
            return null;
        }
        final SqlTypeName typeName = literal.getType().getSqlTypeName();
        switch (typeName) {
            case BOOLEAN:
                return ((Boolean) value).toString();
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case REAL:
            case DOUBLE:
            case DECIMAL:
                // toPlainString avoids scientific notation so that the C++ side (stoll/stod)
                // can parse the value reliably.
                return value instanceof BigDecimal
                        ? ((BigDecimal) value).toPlainString()
                        : value.toString();
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final String valueStr = value.toString();
                if (typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                        && !valueStr.endsWith("Z")
                        && !valueStr.endsWith("z")) {
                    return valueStr + "Z";
                }
                return valueStr;
            case CHAR:
            case VARCHAR:
                return value instanceof NlsString ? ((NlsString) value).getValue() : value.toString();
            case BINARY:
            case VARBINARY:
                return value instanceof ByteString
                        ? ((ByteString) value).toBase64String()
                        : value.toString();
            default:
                return value.toString();
        }
    }

    @JsonProperty(value = FIELD_NAME_TUPLES)
    public List<List<RexLiteral>> getTuples() {
        return tuples;
    }
}
