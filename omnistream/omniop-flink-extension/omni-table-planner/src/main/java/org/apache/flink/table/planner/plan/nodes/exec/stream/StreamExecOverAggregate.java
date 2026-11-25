/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file
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
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfo;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.over.ProcTimeRangeBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.ProcTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.ProcTimeUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRangeUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRowsUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isRowtimeAttribute;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNode} for time-based over operator.
 *
 * <p>OmniStream native patch: when the OVER window matches the native v1 whitelist
 * (processing-time frames - UNBOUNDED PRECEDING or ROWS N PRECEDING -, no constant
 * arguments, no FILTER clause, no DISTINCT, aggregate functions limited to
 * SUM/COUNT/MAX/MIN over BIGINT, partition keys over the native key whitelist), the
 * transformation description is extended with a JSON payload carrying the matching
 * "processFunction" marker ("ProcTimeUnboundedPrecedingFunction" for the unbounded
 * frame, "ProcTimeRowsBoundedPrecedingFunction" plus the "precedingOffset" window
 * size for the ROWS-bounded frame) and all the metadata the native operator needs.
 * OmniGraphOverride picks the marker up at job-graph time and routes the vertex to
 * the native operator. Everything outside the whitelist keeps the plain planner
 * description and therefore stays on the Java path (silent fallback,
 * byte-identical to stock Flink).
 */
@ExecNodeMetadata(
        name = "stream-exec-over-aggregate",
        version = 1,
        producedTransformations = StreamExecOverAggregate.OVER_AGGREGATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecOverAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamExecOverAggregate.class);

    public static final String OVER_AGGREGATE_TRANSFORMATION = "over-aggregate";

    public static final String FIELD_NAME_OVER_SPEC = "overSpec";

    /** Native marker function name, matched by StreamOperatorFactory (C++). */
    private static final String NATIVE_PROCESS_FUNCTION = "ProcTimeUnboundedPrecedingFunction";

    /**
     * Native marker for the ROWS-bounded variant, matched by StreamOperatorFactory
     * (C++). Its description additionally carries the "precedingOffset" window size
     * (SQL "N PRECEDING" = N+1 rows including the current row).
     */
    private static final String NATIVE_ROWS_BOUNDED_PROCESS_FUNCTION =
            "ProcTimeRowsBoundedPrecedingFunction";

    /**
     * Native v1 aggregate whitelist (mirrors ValidateOverAggOPStrategy).
     *
     * <p>Note SUM0 must be listed explicitly: the planner rewrites SUM(x) into
     * "CASE(COUNT(x) > 0, $SUM0(x), NULL)" before the exec node is built, so
     * the aggregate calls reaching this node are COUNT + SUM0, never a bare
     * SUM. AVG decomposes the same way (with an outer Calc doing the divide),
     * so it is covered by this whitelist too.
     */
    private static final Set<String> NATIVE_AGG_FUNCTIONS =
            new HashSet<>(Arrays.asList("SUM", "SUM0", "COUNT", "MAX", "MIN"));

    /** Native v1 aggregate-argument types for SUM/MAX/MIN. */
    private static final Set<String> NATIVE_AGG_ARG_TYPES =
            new HashSet<>(Collections.singletonList("BIGINT"));

    /** Native v1 partition-key types (mirrors the GroupAggregate key whitelist). */
    private static final Set<String> NATIVE_PARTITION_KEY_TYPES =
            new HashSet<>(Arrays.asList("BIGINT", "INTEGER", "VARCHAR(2147483647)"));

    @JsonProperty(FIELD_NAME_OVER_SPEC)
    private final OverSpec overSpec;

    public StreamExecOverAggregate(
            ReadableConfig tableConfig,
            OverSpec overSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecOverAggregate.class),
                ExecNodeContext.newPersistedConfig(StreamExecOverAggregate.class, tableConfig),
                overSpec,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecOverAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_OVER_SPEC) OverSpec overSpec,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.overSpec = checkNotNull(overSpec);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        if (overSpec.getGroups().size() > 1) {
            throw new TableException("All aggregates must be computed on the same window.");
        }

        final OverSpec.GroupSpec group = overSpec.getGroups().get(0);
        final int[] orderKeys = group.getSort().getFieldIndices();
        final boolean[] isAscendingOrders = group.getSort().getAscendingOrders();
        if (orderKeys.length != 1 || isAscendingOrders.length != 1) {
            throw new TableException("The window can only be ordered by a single time column.");
        }

        if (!isAscendingOrders[0]) {
            throw new TableException("The window can only be ordered in ASCENDING mode.");
        }

        final int[] partitionKeys = overSpec.getPartition().getFieldIndices();
        if (partitionKeys.length > 0 && config.getStateRetentionTime() < 0) {
            LOG.warn(
                    "No state retention interval configured for a query which accumulates state. "
                            + "Please provide a query configuration with valid retention interval to prevent "
                            + "excessive state size. You may specify a retention time of 0 to not clean up the state.");
        }

        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();

        final int orderKey = orderKeys[0];
        final LogicalType orderKeyType = inputRowType.getFields().get(orderKey).getType();
        // check time field && identify window rowtime attribute
        final int rowTimeIdx;
        if (isRowtimeAttribute(orderKeyType)) {
            rowTimeIdx = orderKey;
        } else if (isProctimeAttribute(orderKeyType)) {
            rowTimeIdx = -1;
        } else {
            throw new TableException(
                    "OVER windows' ordering in stream mode must be defined on a time attribute.");
        }

        final List<RexLiteral> constants = overSpec.getConstants();
        final List<String> fieldNames = new ArrayList<>(inputRowType.getFieldNames());
        final List<LogicalType> fieldTypes = new ArrayList<>(inputRowType.getChildren());
        IntStream.range(0, constants.size()).forEach(i -> fieldNames.add("TMP" + i));
        for (int i = 0; i < constants.size(); ++i) {
            fieldNames.add("TMP" + i);
            fieldTypes.add(FlinkTypeFactory.toLogicalType(constants.get(i).getType()));
        }

        final RowType aggInputRowType =
                RowType.of(
                        fieldTypes.toArray(new LogicalType[0]), fieldNames.toArray(new String[0]));

        final boolean unboundedFrame =
                group.getLowerBound().isPreceding()
                        && group.getLowerBound().isUnbounded()
                        && group.getUpperBound().isCurrentRow();

        // The bounded native-eligible frame: ROWS N PRECEDING -> CURRENT ROW.
        // The RANGE bounded window needs timer-deferred emission and stays on
        // the Java path in v1.
        final boolean boundedRowsFrame =
                group.getLowerBound().isPreceding()
                        && !group.getLowerBound().isUnbounded()
                        && group.getUpperBound().isCurrentRow()
                        && group.isRows();

        // Window size in rows for the ROWS-bounded frame, assigned in the
        // bounded branch below (N+1 rows including the current row); 0 marks
        // "not applicable" (unbounded frame, never injected downstream).
        long precedingOffset = 0L;

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader());
        final KeyedProcessFunction<RowData, RowData, RowData> overProcessFunction;
        if (unboundedFrame) {
            // unbounded OVER window
            overProcessFunction =
                    createUnboundedOverProcessFunction(
                            ctx,
                            group.getAggCalls(),
                            constants,
                            aggInputRowType,
                            inputRowType,
                            rowTimeIdx,
                            group.isRows(),
                            config,
                            planner.createRelBuilder(),
                            planner.getTypeFactory());
        } else if (group.getLowerBound().isPreceding()
                && !group.getLowerBound().isUnbounded()
                && group.getUpperBound().isCurrentRow()) {
            final Object boundValue =
                    OverAggregateUtil.getBoundary(overSpec, group.getLowerBound());

            if (boundValue instanceof BigDecimal) {
                throw new TableException(
                        "the specific value is decimal which haven not supported yet.");
            }
            // bounded OVER window
            precedingOffset = -1 * (long) boundValue + (group.isRows() ? 1 : 0);
            overProcessFunction =
                    createBoundedOverProcessFunction(
                            ctx,
                            group.getAggCalls(),
                            constants,
                            aggInputRowType,
                            inputRowType,
                            rowTimeIdx,
                            group.isRows(),
                            precedingOffset,
                            config,
                            planner.createRelBuilder(),
                            planner.getTypeFactory());
        } else {
            throw new TableException("OVER RANGE FOLLOWING windows are not supported yet.");
        }

        final KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(overProcessFunction);

        OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(OVER_AGGREGATE_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism());

        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        partitionKeys,
                        InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());

        // OmniStream native path: only the v1 whitelist (proc-time UNBOUNDED
        // PRECEDING or ROWS N PRECEDING, plain single-argument
        // SUM/COUNT/MAX/MIN over BIGINT, native key types, no constants /
        // FILTER / DISTINCT) receives the JSON description; everything else
        // keeps the plain planner description and falls back to Java at
        // graph-validation time.
        if (isNativeEligible(unboundedFrame, boundedRowsFrame, rowTimeIdx, constants, group, inputRowType)) {
            final AggregateInfoList aggInfoList =
                    AggregateUtil.transformToStreamAggregateInfoList(
                            planner.getTypeFactory(),
                            aggInputRowType,
                            JavaScalaConversionUtil.toScala(group.getAggCalls()),
                            new boolean[group.getAggCalls().size()],
                            false,
                            true,
                            true);
            final LogicalType[] accTypes =
                    Arrays.stream(aggInfoList.getAccTypes())
                            .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                            .toArray(LogicalType[]::new);
            final LogicalType[] aggValueTypes =
                    Arrays.stream(aggInfoList.getActualValueTypes())
                            .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                            .toArray(LogicalType[]::new);
            String oldDescription = transform.getDescription();
            transform.setDescription(
                    getExtraDescription(
                            oldDescription,
                            aggInfoList,
                            accTypes,
                            aggValueTypes,
                            inputTransform,
                            partitionKeys,
                            unboundedFrame
                                    ? NATIVE_PROCESS_FUNCTION
                                    : NATIVE_ROWS_BOUNDED_PROCESS_FUNCTION,
                            unboundedFrame ? null : Long.valueOf(precedingOffset)));
            LOG.info("OverAggregate native description injected for {}", oldDescription);
        }

        return transform;
    }

    /**
     * Native v1 eligibility. Must stay in sync with the C++ operator whitelist
     * (ProcTimeUnboundedPrecedingFunction / ProcTimeRowsBoundedPrecedingFunction)
     * and with ValidateOverAggOPStrategy.
     */
    private boolean isNativeEligible(
            boolean unboundedFrame,
            boolean boundedRowsFrame,
            int rowTimeIdx,
            List<RexLiteral> constants,
            OverSpec.GroupSpec group,
            RowType inputRowType) {
        // Processing time only: rowtime windows need watermark logic.
        if (rowTimeIdx >= 0) {
            return false;
        }
        // v1 native frames: UNBOUNDED PRECEDING -> CURRENT ROW, or the ROWS
        // N PRECEDING -> CURRENT ROW bounded frame. The RANGE bounded frame
        // needs timer-deferred emission and stays on the Java path.
        if (!unboundedFrame && !boundedRowsFrame) {
            return false;
        }
        // v1 does not model constant arguments like SUM(1) (TMP columns).
        if (!constants.isEmpty()) {
            return false;
        }
        for (AggregateCall aggCall : group.getAggCalls()) {
            String functionName = extractAggFunctionName(aggCall);
            if (!NATIVE_AGG_FUNCTIONS.contains(functionName)) {
                return false;
            }
            // v1 has no FILTER / DISTINCT support in the native operator.
            if (aggCall.filterArg >= 0 || aggCall.isDistinct()) {
                return false;
            }
            // COUNT(*) has no arguments; COUNT(x) is fine for any input type
            // (the native CountFunction only counts rows).
            if (aggCall.getArgList().isEmpty()) {
                continue;
            }
            if (aggCall.getArgList().size() != 1) {
                return false;
            }
            if (!"COUNT".equals(functionName)) {
                int argIndex = aggCall.getArgList().get(0);
                LogicalType argType = inputRowType.getFields().get(argIndex).getType();
                String typeName = argType.getTypeRoot().toString();
                if (!NATIVE_AGG_ARG_TYPES.contains(typeName)) {
                    return false;
                }
            }
        }
        // Partition keys must be native-keyable (same list as GroupAggregate).
        for (int keyIndex : overSpec.getPartition().getFieldIndices()) {
            LogicalType keyType = inputRowType.getFields().get(keyIndex).getType();
            String typeName = keyType.getTypeRoot().toString();
            if (LogicalTypeRoot.VARCHAR.equals(keyType.getTypeRoot())) {
                typeName = "VARCHAR(2147483647)";
            }
            if (!NATIVE_PARTITION_KEY_TYPES.contains(typeName)) {
                return false;
            }
        }
        return true;
    }

    private static String extractAggFunctionName(AggregateCall aggCall) {
        String name = aggCall.getAggregation().kind.toString();
        // planner variants like $SUM0 keep the $ prefix; strip it so the
        // whitelist (which lists SUM0 explicitly) can match the base name.
        if (name.contains("$")) {
            name = name.replace("$", "");
        }
        return name.toUpperCase(Locale.ROOT);
    }

    private String getExtraDescription(
            String oldDescription,
            AggregateInfoList aggInfoList,
            LogicalType[] accTypes,
            LogicalType[] aggValueTypes,
            Transformation<RowData> inputTransform,
            int[] partitionKeys,
            String processFunction,
            Long precedingOffset) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        List<String> inputTypeList = getInputTypes(inputTransform);
        List<String> outputTypeList = getOutputTypes();

        // aggInfoList info map (same shape as StreamExecGroupAggregate)
        Map<String, Object> aggInfoListMap = new LinkedHashMap<>();
        aggInfoListMap.put("aggregateCalls", getAggregateCalls(aggInfoList));
        List<String> accTypesList = new ArrayList<>();
        for (LogicalType accType : accTypes) {
            accTypesList.add(DescriptionUtil.toAccTypeString(accType));
        }
        aggInfoListMap.put("accTypes", accTypesList);
        List<String> aggValueTypesList = new ArrayList<>();
        for (LogicalType accType : aggValueTypes) {
            aggValueTypesList.add(accType.toString());
        }
        aggInfoListMap.put("aggValueTypes", aggValueTypesList);
        aggInfoListMap.put("indexOfCountStar", aggInfoList.getIndexOfCountStar());
        // The native v1 whitelist excludes DISTINCT, so this is always empty.
        aggInfoListMap.put("distinctInfos", new ArrayList<Map<String, Object>>());

        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("processFunction", processFunction);
        // ROWS-bounded contract with the native operator: the injected window
        // size is the same precedingOffset the Java function consumes
        // (SQL "N PRECEDING" = N+1 rows including the current row); null
        // (unbounded frame) omits the field.
        if (precedingOffset != null) {
            jsonMap.put("precedingOffset", precedingOffset);
        }
        jsonMap.put("originDescription", oldDescription);
        jsonMap.put("inputTypes", inputTypeList);
        jsonMap.put("outputTypes", outputTypeList);
        jsonMap.put("grouping", partitionKeys);
        jsonMap.put("aggInfoList", aggInfoListMap);
        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            LOG.warn("OverAggregate getExtraDescription error", e);
        }
        return jsonString;
    }

    private static List<Map<String, Object>> getAggregateCalls(AggregateInfoList aggInfoList) {
        AggregateInfo[] aggInfos = aggInfoList.getActualAggregateInfos();
        List<Map<String, Object>> aggregateCalls = new ArrayList<>();
        for (AggregateInfo aggInfo : aggInfos) {
            Map<String, Object> aggregateCallMap = new LinkedHashMap<>();
            AggregateCall aggCall = aggInfo.agg();
            aggregateCallMap.put("name", (aggCall != null ? aggCall.toString() : "null"));
            UserDefinedFunction function = aggInfo.function();
            aggregateCallMap.put("aggregationFunction", (function != null ? function.toString() : "null"));
            aggregateCallMap.put("argIndexes", aggInfo.argIndexes());
            aggregateCallMap.put("consumeRetraction", Boolean.toString(aggInfo.consumeRetraction()));
            // The native operator requires an integer filterArg; the v1
            // whitelist guarantees FILTER is absent, so encode "no filter"
            // as -1 instead of the "null" string the GroupAggregate patch uses.
            aggregateCallMap.put("filterArg", (aggCall != null && aggCall.filterArg >= 0
                    ? aggCall.filterArg : -1));
            aggregateCalls.add(aggregateCallMap);
        }
        return aggregateCalls;
    }

    private List<String> getOutputTypes() {
        List<String> outputTypeList = new ArrayList<>();
        List<RowType.RowField> fields = ((RowType) getOutputType()).getFields();
        for (RowType.RowField field : fields) {
            LogicalType fieldType = field.getType();
            LogicalTypeRoot typeRoot = fieldType.getTypeRoot();
            String typeName = typeRoot.toString();
            if (typeRoot == LogicalTypeRoot.VARCHAR) {
                VarCharType varcharType = (VarCharType) fieldType;
                typeName += "(" + varcharType.getLength() + ")";
            }
            if (typeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                    || typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE
                    || typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {

                // Cast to TimestampType to access precision
                if (fieldType instanceof TimestampType) {
                    int precision = ((TimestampType) fieldType).getPrecision();
                    typeName += "(" + precision + ")";
                }
                if (fieldType instanceof LocalZonedTimestampType) {
                    int precision = ((LocalZonedTimestampType) fieldType).getPrecision();
                    typeName += "(" + precision + ")";
                }
            }
            outputTypeList.add(typeName);
        }
        return outputTypeList;
    }

    private static List<String> getInputTypes(Transformation<RowData> inputTransform) {
        List<String> inputTypeList = new ArrayList<>();
        List<RowType.RowField> inputFields =
                ((InternalTypeInfo) inputTransform.getOutputType()).toRowType().getFields();
        for (RowType.RowField field : inputFields) {
            LogicalType fieldType = field.getType();
            LogicalTypeRoot typeRoot = fieldType.getTypeRoot();
            String typeName = typeRoot.toString();
            if (typeRoot == LogicalTypeRoot.VARCHAR) {
                VarCharType varcharType = (VarCharType) fieldType;
                typeName += "(" + varcharType.getLength() + ")";
            }
            if (typeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                    || typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE
                    || typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {

                // Cast to TimestampType to access precision
                if (fieldType instanceof TimestampType) {
                    int precision = ((TimestampType) fieldType).getPrecision();
                    typeName += "(" + precision + ")";
                }
                if (fieldType instanceof LocalZonedTimestampType) {
                    int precision = ((LocalZonedTimestampType) fieldType).getPrecision();
                    typeName += "(" + precision + ")";
                }
            }
            inputTypeList.add(typeName);
        }
        return inputTypeList;
    }

    /**
     * Create an ProcessFunction for unbounded OVER window to evaluate final aggregate value.
     *
     * @param ctx code generator context
     * @param aggCalls physical calls to aggregate functions and their output field names
     * @param constants the constants in aggregates parameters, such as sum(1)
     * @param aggInputRowType physical type of the input row which consists of input and constants.
     * @param inputRowType physical type of the input row which only consists of input.
     * @param rowTimeIdx the index of the rowtime field or None in case of processing time.
     * @param isRowsClause it is a tag that indicates whether the OVER clause is ROWS clause
     */
    private KeyedProcessFunction<RowData, RowData, RowData> createUnboundedOverProcessFunction(
            CodeGeneratorContext ctx,
            List<AggregateCall> aggCalls,
            List<RexLiteral> constants,
            RowType aggInputRowType,
            RowType inputRowType,
            int rowTimeIdx,
            boolean isRowsClause,
            ExecNodeConfig config,
            RelBuilder relBuilder,
            FlinkTypeFactory typeFactory) {
        AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        typeFactory,
                        // use aggInputType which considers constants as input instead of
                        // inputSchema.relDataType
                        aggInputRowType,
                        JavaScalaConversionUtil.toScala(aggCalls),
                        new boolean[aggCalls.size()],
                        false,
                        true,
                        true);

        LogicalType[] fieldTypes = inputRowType.getChildren().toArray(new LogicalType[0]);
        AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        ctx,
                        relBuilder,
                        JavaScalaConversionUtil.toScala(Arrays.asList(fieldTypes)),
                        false);

        GeneratedAggsHandleFunction genAggsHandler =
                generator
                        .needAccumulate()
                        // over agg code gen must pass the constants
                        .withConstants(JavaScalaConversionUtil.toScala(constants))
                        .generateAggsHandler("UnboundedOverAggregateHelper", aggInfoList);

        LogicalType[] flattenAccTypes =
                Arrays.stream(aggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);

        if (rowTimeIdx >= 0) {
            if (isRowsClause) {
                // ROWS unbounded over process function
                return new RowTimeRowsUnboundedPrecedingFunction<>(
                        config.getStateRetentionTime(),
                        TableConfigUtils.getMaxIdleStateRetentionTime(config),
                        genAggsHandler,
                        flattenAccTypes,
                        fieldTypes,
                        rowTimeIdx);
            } else {
                // RANGE unbounded over process function
                return new RowTimeRangeUnboundedPrecedingFunction<>(
                        config.getStateRetentionTime(),
                        TableConfigUtils.getMaxIdleStateRetentionTime(config),
                        genAggsHandler,
                        flattenAccTypes,
                        fieldTypes,
                        rowTimeIdx);
            }
        } else {
            return new ProcTimeUnboundedPrecedingFunction<>(
                    config.getStateRetentionTime(),
                    TableConfigUtils.getMaxIdleStateRetentionTime(config),
                    genAggsHandler,
                    flattenAccTypes);
        }
    }

    /**
     * Create an ProcessFunction for ROWS clause bounded OVER window to evaluate final aggregate
     * value.
     *
     * @param ctx code generator context
     * @param aggCalls physical calls to aggregate functions and their output field names
     * @param constants the constants in aggregates parameters, such as sum(1)
     * @param aggInputType physical type of the input row which consists of input and constants.
     * @param inputType physical type of the input row which only consists of input.
     * @param rowTimeIdx the index of the rowtime field or None in case of processing time.
     * @param isRowsClause it is a tag that indicates whether the OVER clause is ROWS clause
     */
    private KeyedProcessFunction<RowData, RowData, RowData> createBoundedOverProcessFunction(
            CodeGeneratorContext ctx,
            List<AggregateCall> aggCalls,
            List<RexLiteral> constants,
            RowType aggInputType,
            RowType inputType,
            int rowTimeIdx,
            boolean isRowsClause,
            long precedingOffset,
            ExecNodeConfig config,
            RelBuilder relBuilder,
            FlinkTypeFactory typeFactory) {

        boolean[] aggCallNeedRetractions = new boolean[aggCalls.size()];
        Arrays.fill(aggCallNeedRetractions, true);
        AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        typeFactory,
                        // use aggInputType which considers constants as input instead of
                        // inputSchema.relDataType
                        aggInputType,
                        JavaScalaConversionUtil.toScala(aggCalls),
                        aggCallNeedRetractions,
                        true,
                        true,
                        true);

        LogicalType[] fieldTypes = inputType.getChildren().toArray(new LogicalType[0]);
        AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                        ctx,
                        relBuilder,
                        JavaScalaConversionUtil.toScala(Arrays.asList(fieldTypes)),
                        false);

        GeneratedAggsHandleFunction genAggsHandler =
                generator
                        .needRetract()
                        .needAccumulate()
                        // over agg code gen must pass the constants
                        .withConstants(JavaScalaConversionUtil.toScala(constants))
                        .generateAggsHandler("BoundedOverAggregateHelper", aggInfoList);

        LogicalType[] flattenAccTypes =
                Arrays.stream(aggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);

        if (rowTimeIdx >= 0) {
            if (isRowsClause) {
                return new RowTimeRowsBoundedPrecedingFunction<>(
                        config.getStateRetentionTime(),
                        TableConfigUtils.getMaxIdleStateRetentionTime(config),
                        genAggsHandler,
                        flattenAccTypes,
                        fieldTypes,
                        precedingOffset,
                        rowTimeIdx);
            } else {
                return new RowTimeRangeBoundedPrecedingFunction<>(
                        genAggsHandler, flattenAccTypes, fieldTypes, precedingOffset, rowTimeIdx);
            }
        } else {
            if (isRowsClause) {
                return new ProcTimeRowsBoundedPrecedingFunction<>(
                        config.getStateRetentionTime(),
                        TableConfigUtils.getMaxIdleStateRetentionTime(config),
                        genAggsHandler,
                        flattenAccTypes,
                        fieldTypes,
                        precedingOffset);
            } else {
                return new ProcTimeRangeBoundedPrecedingFunction<>(
                        genAggsHandler, flattenAccTypes, fieldTypes, precedingOffset);
            }
        }
    }
}
