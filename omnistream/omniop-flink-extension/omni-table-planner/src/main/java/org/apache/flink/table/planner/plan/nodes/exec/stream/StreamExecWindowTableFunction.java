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
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import static org.apache.flink.table.planner.plan.utils.WindowTableFunctionUtil.createWindowAssigner;
import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecWindowTableFunction;
import org.apache.flink.table.planner.plan.nodes.exec.util.DescriptionUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.WindowTableFunctionOperator;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Stream {@link ExecNode} which acts as a table-valued function to assign a window for each row of
 * the input relation. The return value of the new relation includes all the original columns as
 * well additional 3 columns named {@code window_start}, {@code window_end}, {@code window_time} to
 * indicate the assigned window.
 *
 * <p>This class overrides the Flink {@link
 * org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowTableFunction} to attach a
 * native operator description (JSON) for OmniStream execution.
 */
@ExecNodeMetadata(
        name = "stream-exec-window-table-function",
        version = 1,
        consumedOptions = "table.local-time-zone",
        producedTransformations = CommonExecWindowTableFunction.WINDOW_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecWindowTableFunction extends CommonExecWindowTableFunction
        implements StreamExecNode<RowData> {

    public StreamExecWindowTableFunction(
            ReadableConfig tableConfig,
            TimeAttributeWindowingStrategy windowingStrategy,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecWindowTableFunction.class),
                ExecNodeContext.newPersistedConfig(
                        StreamExecWindowTableFunction.class, tableConfig),
                windowingStrategy,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecWindowTableFunction(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_WINDOWING) TimeAttributeWindowingStrategy windowingStrategy,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                windowingStrategy,
                inputProperties,
                outputType,
                description);
        checkNotNull(windowingStrategy);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        // 与 Flink 1.16.3 CommonExecWindowTableFunction#translateToPlanInternal 结构保持一致：
        // 用迁移到 OmniAdaptor 的 WindowTableFunctionUtil#createWindowAssigner 在 Java 侧完成
        // window assigner 的构造（TUMBLE/HOP/CUMULATE 分派），再组装 WindowTableFunctionOperator
        // 生成 Transformation 骨架，最后附加原生算子描述。语义与上游一致。
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        WindowAssigner<TimeWindow> windowAssigner = createWindowAssigner(windowingStrategy);
        final ZoneId shiftTimeZone =
                TimeWindowUtil.getShiftTimeZone(
                        windowingStrategy.getTimeAttributeType(),
                        TableConfigUtils.getLocalTimeZone(config));
        WindowTableFunctionOperator windowTableFunctionOperator =
                new WindowTableFunctionOperator(
                        windowAssigner, windowingStrategy.getTimeAttributeIndex(), shiftTimeZone);
        Transformation<RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(CommonExecWindowTableFunction.WINDOW_TRANSFORMATION, config),
                        windowTableFunctionOperator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism());
        transform.setDescription(getExtraDescription(transform, config));
        return transform;
    }

    private String getExtraDescription(Transformation<RowData> transform, ExecNodeConfig config) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        jsonMap.put("originDescription", transform.getDescription());

        RowType inputRowType = (RowType) getInputEdges().get(0).getOutputType();
        List<String> inputTypeList = DescriptionUtil.getFieldTypeList(inputRowType.getFields());
        List<String> outputTypeList =
                DescriptionUtil.getFieldTypeList(((RowType) getOutputType()).getFields());
        jsonMap.put("inputTypes", inputTypeList);
        jsonMap.put("outputTypes", outputTypeList);

        ZoneId shiftTimeZone =
                TimeWindowUtil.getShiftTimeZone(
                        windowingStrategy.getTimeAttributeType(),
                        TableConfigUtils.getLocalTimeZone(config));
        jsonMap.put("window", windowingStrategy.getWindow().toString());
        jsonMap.put("timeType", windowingStrategy.isRowtime() ? "event" : "processing");
        jsonMap.put(
                "timeAttributeType",
                DescriptionUtil.getFieldType(windowingStrategy.getTimeAttributeType()));

        int timeAttributeIndex =
                windowingStrategy.isRowtime() ? windowingStrategy.getTimeAttributeIndex() : -1;
        buildJsonMap(windowingStrategy.getWindow(), timeAttributeIndex, shiftTimeZone, jsonMap);

        String jsonString;
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            throw new TableException("Failed to serialize WindowTableFunction description", e);
        }
        return jsonString;
    }

    private void buildJsonMap(
            WindowSpec windowSpec,
            int timeAttributeIndex,
            ZoneId shiftTimeZone,
            Map<String, Object> jsonMap) {
        if (windowSpec instanceof TumblingWindowSpec) {
            Duration size = ((TumblingWindowSpec) windowSpec).getSize();
            Duration offset = ((TumblingWindowSpec) windowSpec).getOffset();
            if (offset != null) {
                jsonMap.put("windowOffset", offset.toMillis());
            }
            jsonMap.put("timeAttributeIndex", timeAttributeIndex);
            jsonMap.put("windowSize", size.toMillis());
            jsonMap.put("windowType", "TUMBLE");
        } else if (windowSpec instanceof HoppingWindowSpec) {
            Duration size = ((HoppingWindowSpec) windowSpec).getSize();
            Duration slide = ((HoppingWindowSpec) windowSpec).getSlide();
            Duration offset = ((HoppingWindowSpec) windowSpec).getOffset();
            if (offset != null) {
                jsonMap.put("windowOffset", offset.toMillis());
            }
            jsonMap.put("timeAttributeIndex", timeAttributeIndex);
            jsonMap.put("windowSize", size.toMillis());
            jsonMap.put("windowSlide", slide.toMillis());
            jsonMap.put("windowType", "HOP");
        } else if (windowSpec instanceof CumulativeWindowSpec) {
            Duration maxSize = ((CumulativeWindowSpec) windowSpec).getMaxSize();
            Duration step = ((CumulativeWindowSpec) windowSpec).getStep();
            Duration offset = ((CumulativeWindowSpec) windowSpec).getOffset();
            if (offset != null) {
                jsonMap.put("windowOffset", offset.toMillis());
            }
            jsonMap.put("timeAttributeIndex", timeAttributeIndex);
            jsonMap.put("maxSize", maxSize.toMillis());
            jsonMap.put("step", step.toMillis());
            jsonMap.put("windowType", "CUMULATE");
        } else {
            throw new UnsupportedOperationException(windowSpec + " is not supported yet.");
        }
        jsonMap.put("shiftTimeZone", shiftTimeZone.toString());
    }
}
