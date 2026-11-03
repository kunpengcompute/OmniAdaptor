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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.UserDefinedFunction;
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
import org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfo;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.DistinctInfo;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedTableAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.GroupTableAggFunction;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Stream {@link ExecNode} for unbounded group TABLE aggregate (the {@code LATERAL TABLE}
 * syntax with a {@code TableAggregateFunction} UDF + GROUP BY).
 *
 * <p>OmniStream native adaptation (v1): when the single aggregate call is the whitelisted
 * Top2 UDF over a BIGINT column, the plan info is serialized into the transformation
 * description ({@link #getExtraDescription}) exactly in the schema the native
 * {@code GroupTableAggregateFunction::parseDescription} understands, and the operator can
 * be replaced by the C++ implementation. Any other UDF produces no {@code processFunction}
 * field, so the operator is never replaced and keeps the plain Java path. This mirrors the
 * count_char scalar-UDF point-name whitelist precedent (group.sql / Nexmark Q14).
 */
@ExecNodeMetadata(
        name = "stream-exec-group-table-aggregate",
        version = 1,
        producedTransformations = StreamExecGroupTableAggregate.GROUP_TABLE_AGGREGATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecGroupTableAggregate extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    public static final String GROUP_TABLE_AGGREGATE_TRANSFORMATION = "group-table-aggregate";
    private static final Logger LOG = LoggerFactory.getLogger(StreamExecGroupTableAggregate.class);

    /** The only table-aggregate UDF adapted natively in v1 (point-name whitelist). */
    public static final String NATIVE_UDF_SIMPLE_NAME = "Top2";

    /** Dispatch key understood by the OmniStream StreamOperatorFactory. */
    public static final String PROCESS_FUNCTION = "GroupTableAggregateFunction";

    /** Top2 rank bounds serialized for the native side (descending, top-2). */
    private static final String RANK_RANGE = "rankStart=1, rankEnd=2";

    // JSON field names kept identical to StreamExecAggregateBase (that interface is not
    // implemented here because this node is not part of the plain-aggregate family).
    private static final String FIELD_NAME_GROUPING = "grouping";
    private static final String FIELD_NAME_AGG_CALLS = "aggCalls";
    private static final String FIELD_NAME_AGG_CALL_NEED_RETRACTIONS = "aggCallNeedRetractions";
    private static final String FIELD_NAME_GENERATE_UPDATE_BEFORE = "generateUpdateBefore";
    private static final String FIELD_NAME_NEED_RETRACTION = "needRetraction";

    @JsonProperty(FIELD_NAME_GROUPING)
    private final int[] grouping;

    @JsonProperty(FIELD_NAME_AGG_CALLS)
    private final AggregateCall[] aggCalls;

    /** Each element indicates whether the corresponding agg call needs `retract` method. */
    @JsonProperty(FIELD_NAME_AGG_CALL_NEED_RETRACTIONS)
    private final boolean[] aggCallNeedRetractions;

    /** Whether this node will generate UPDATE_BEFORE messages. */
    @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE)
    private final boolean generateUpdateBefore;

    /** Whether this node consumes retraction messages. */
    @JsonProperty(FIELD_NAME_NEED_RETRACTION)
    private final boolean needRetraction;

    public StreamExecGroupTableAggregate(
            ReadableConfig tableConfig,
            int[] grouping,
            AggregateCall[] aggCalls,
            boolean[] aggCallNeedRetractions,
            boolean generateUpdateBefore,
            boolean needRetraction,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecGroupTableAggregate.class),
                ExecNodeContext.newPersistedConfig(StreamExecGroupTableAggregate.class, tableConfig),
                grouping,
                aggCalls,
                aggCallNeedRetractions,
                generateUpdateBefore,
                needRetraction,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecGroupTableAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
            @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
            @JsonProperty(FIELD_NAME_AGG_CALL_NEED_RETRACTIONS) boolean[] aggCallNeedRetractions,
            @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE) boolean generateUpdateBefore,
            @JsonProperty(FIELD_NAME_NEED_RETRACTION) boolean needRetraction,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.grouping = grouping;
        this.aggCalls = aggCalls;
        this.aggCallNeedRetractions = aggCallNeedRetractions;
        org.apache.flink.util.Preconditions
                .checkArgument(aggCalls.length == aggCallNeedRetractions.length);
        this.generateUpdateBefore = generateUpdateBefore;
        this.needRetraction = needRetraction;
    }

    /**
     * Single source of truth for native eligibility (also reused by the validate strategy):
     * the plan runs natively iff it is exactly one whitelisted Top2 call over one BIGINT
     * column. Everything else keeps the Java path.
     */
    public static boolean isNativeTop2Plan(
            AggregateCall[] aggCalls, AggregateInfoList aggInfoList, RowType inputRowType) {
        if (aggCalls == null || aggCalls.length != 1) {
            return false;
        }
        final AggregateCall call = aggCalls[0];
        // Top2(v): exactly one argument, the sort key column.
        if (call.getArgList().size() != 1) {
            return false;
        }
        final int argIndex = call.getArgList().get(0);
        if (argIndex < 0 || argIndex >= inputRowType.getFieldCount()) {
            return false;
        }
        // v1 native scope: BIGINT sort key only (matches RowNumberFunction v1).
        if (inputRowType.getTypeAt(argIndex).getTypeRoot() != LogicalTypeRoot.BIGINT) {
            return false;
        }
        final AggregateInfo[] aggInfos = aggInfoList.getActualAggregateInfos();
        if (aggInfos.length != 1 || aggInfos[0] == null || aggInfos[0].function() == null) {
            return false;
        }
        UserDefinedFunction function = aggInfos[0].function();
        return NATIVE_UDF_SIMPLE_NAME.equals(function.getClass().getSimpleName());
    }

    private String getExtraDescription(
            String oldDescription,
            AggregateInfoList aggInfoList,
            LogicalType[] accTypes,
            LogicalType[] aggValueTypes,
            Transformation<RowData> inputTransform,
            RowType inputRowType,
            String name) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        List<String> inputTypeList = getInputTypes(inputTransform);

        List<String> outputTypeList = getOutputTypes();

        // get aggInfoList info map
        Map<String, Object> aggInfoListMap = new LinkedHashMap<>();
        List<Map<String, Object>> aggregateCalls = getAggregateCalls(aggInfoList);
        aggInfoListMap.put("aggregateCalls", aggregateCalls);
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

        // distinctInfo List Map
        List<Map<String, Object>> distinctInfos = getDistinctInfos(aggInfoList);

        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("originDescription", oldDescription);
        jsonMap.put("inputTypes", inputTypeList);
        jsonMap.put("outputTypes", outputTypeList);
        jsonMap.put("grouping", grouping);
        jsonMap.put("aggInfoList", aggInfoListMap);
        jsonMap.put("generateUpdateBefore", generateUpdateBefore);
        jsonMap.put("distinctInfos", distinctInfos);
        if (name.contains("$e")) {
            jsonMap.put("groupingSets", "INVALID");
        }

        // ---- OmniStream native dispatch block (schema: GroupTableAggregateFunction::parseDescription).
        // Top2 point-name whitelist only: any other table-aggregate UDF gets no processFunction
        // field and therefore never gets replaced (falls back to the plain Java operator). ----
        if (isNativeTop2Plan(aggCalls, aggInfoList, inputRowType)) {
            final AggregateCall call = aggCalls[0];
            final int sortFieldIndex = call.getArgList().get(0);
            jsonMap.put("processFunction", PROCESS_FUNCTION);
            jsonMap.put("sortFieldIndices", Collections.singletonList(sortFieldIndex));
            // Top2 keeps the two largest values: descending order.
            jsonMap.put("sortAscendingOrders", Collections.singletonList(false));
            jsonMap.put("rankRange", RANK_RANGE);
            // uniqueKeyIndex is intentionally omitted: the Top2 acceptance SQL input row is
            // [grouping key, UDF argument] with no identity column, and the native side runs
            // its keyless mode (plain Flink Top2 semantics: a repeated value ranks twice).
            LOG.info("GroupTableAggregate plan serialized for native Top2 execution");
        } else {
            LOG.info(
                    "GroupTableAggregate keeps the Java path: the aggregate UDF is not the whitelisted {}",
                    NATIVE_UDF_SIMPLE_NAME);
        }

        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            LOG.warn("getExtraDescription error", e);
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
            aggregateCallMap.put("filterArg", (aggCall != null ? aggCall.filterArg : "null"));
            aggregateCalls.add(aggregateCallMap);
        }
        return aggregateCalls;
    }

    private static List<Map<String, Object>> getDistinctInfos(AggregateInfoList aggInfoList) {
        List<Map<String, Object>> distinctInfos = new ArrayList<>();
        for (DistinctInfo distinctInfo : aggInfoList.distinctInfos()) {
            Map<String, Object> distinctInfoMap = new LinkedHashMap<>();
            distinctInfoMap.put("argIndexes", distinctInfo.argIndexes());
            distinctInfoMap.put("filterArgs", distinctInfo.filterArgs().toArray(ClassTag$.MODULE$.apply(Integer.TYPE)));
            distinctInfoMap.put("aggIndexes", distinctInfo.aggIndexes().toArray(ClassTag$.MODULE$.apply(Integer.TYPE)));
            distinctInfos.add(distinctInfoMap);
        }
        return distinctInfos;
    }

    private List<String> getOutputTypes() {
        List<String> outputTypeList = new ArrayList<>();
        List<RowType.RowField> fields = ((RowType) getOutputType()).getFields();
        for (RowType.RowField field : fields) {
            outputTypeList.add(field.getType().asSummaryString());
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

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        if (grouping.length > 0 && config.getStateRetentionTime() < 0) {
            LOG.warn(
                    "No state retention interval configured for a query which accumulates state. "
                            + "Please provide a query configuration with valid retention interval to prevent excessive "
                            + "state size. You may specify a retention time of 0 to not clean up the state.");
        }

        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();

        final AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                                new CodeGeneratorContext(
                                        config, planner.getFlinkContext().getClassLoader()),
                                planner.createRelBuilder(),
                                JavaScalaConversionUtil.toScala(inputRowType.getChildren()),
                                true)
                        .needAccumulate();

        if (needRetraction) {
            generator.needRetract();
        }

        final AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        planner.getTypeFactory(),
                        inputRowType,
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),
                        aggCallNeedRetractions,
                        needRetraction,
                        true, // isStateBackendDataViews
                        true); // needDistinctInfo
        final GeneratedTableAggsHandleFunction aggsHandler =
                generator.generateTableAggsHandler("GroupTableAggHandler", aggInfoList);

        final LogicalType[] accTypes =
                Arrays.stream(aggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);
        final int inputCountIndex = aggInfoList.getIndexOfCountStar();
        final GroupTableAggFunction aggFunction =
                new GroupTableAggFunction(
                        aggsHandler,
                        accTypes,
                        inputCountIndex,
                        generateUpdateBefore,
                        config.getStateRetentionTime());
        final OneInputStreamOperator<RowData, RowData> operator = new KeyedProcessOperator<>(aggFunction);

        // partitioned aggregation
        final OneInputTransformation<RowData, RowData> transform =
                new OneInputTransformation<>(
                        inputTransform,
                        getDescription(),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism());

        // set KeyType and Selector for state
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        grouping,
                        InternalTypeInfo.of(inputRowType));
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        String oldDescription = transform.getDescription();
        transform.setDescription(
                getExtraDescription(
                        oldDescription,
                        aggInfoList,
                        accTypes,
                        new LogicalType[0],
                        inputTransform,
                        inputRowType,
                        transform.getName()));

        return transform;
    }
}
