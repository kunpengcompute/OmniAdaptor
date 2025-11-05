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

package com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain;

import static org.apache.flink.util.Preconditions.checkState;

import com.huawei.omniruntime.flink.runtime.api.graph.json.JsonHelper;
import com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.tablescan.CSVFormatPOJO;
import com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.tablescan.NexmarkFormatPOJO;
import com.huawei.omniruntime.flink.table.types.logical.LogicalTypeDescriptor;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.CharSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * OperatorChainDescriptorHelper
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class OperatorChainDescriptorHelper {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorChainDescriptorHelper.class);

    /**
     * retrieveOperatorChain
     *
     * @param taskConfig task Config
     * @param userCodeClassloader user CodeClassloader
     * @return {@link OperatorChainPOJO }
     */
    public static OperatorChainPOJO retrieveOperatorChain(StreamConfig taskConfig, ClassLoader userCodeClassloader) {

        // main operator factory
        StreamOperatorFactory mainOperatorFactory =
                taskConfig.getStreamOperatorFactory(userCodeClassloader);

        LOG.info("mainOperatorFactory class name {} and getStreamOperatorClass {}", mainOperatorFactory.getClass().getCanonicalName(),
                mainOperatorFactory.getStreamOperatorClass(userCodeClassloader).getCanonicalName());

        OperatorPOJO mainOpDesc = createNonChainedOperatorDescriptor(mainOperatorFactory, userCodeClassloader, taskConfig);

        // body and tail
        Map<Integer, StreamConfig> chainedConfigs =
                taskConfig.getTransitiveChainedTaskConfigsWithSelf(userCodeClassloader);
        List<OperatorPOJO> operators = new ArrayList<>();
        List<StreamEdge> chainedOutputs = taskConfig.getChainedOutputs(userCodeClassloader);
        recursiveBodyAndTail(chainedOutputs, chainedConfigs, userCodeClassloader, operators);
        // add main op,  order of operator?
        operators.add(mainOpDesc);
        operators.removeIf(opj -> opj.getId().equals("org.apache.flink.streaming.api.operators.StreamFilter")
                || opj.getId().equals("org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer"));
        return new OperatorChainPOJO(operators);
    }

    private static void recursiveBodyAndTail(
            List<StreamEdge> chainedOutputs,
            Map<Integer, StreamConfig> chainedConfigs,
            ClassLoader userCodeClassloader,
            List<OperatorPOJO> operators) {
        if (CollectionUtil.isNullOrEmpty(chainedOutputs)) {
            return;
        }
        for (StreamEdge outputEdge : chainedOutputs) {
            int outputId = outputEdge.getTargetId();
            LOG.info("outputId {}", outputId);
            StreamConfig chainedOpConfig = chainedConfigs.get(outputId);
            OperatorPOJO operatorDescriptor = createOperatorDescriptor(chainedOpConfig, userCodeClassloader, operators, false);
            List<StreamEdge> targetOutputs = chainedOpConfig.getChainedOutputs(userCodeClassloader);
            recursiveBodyAndTail(targetOutputs, chainedConfigs, userCodeClassloader, operators);
            operators.add(operatorDescriptor);
        }
    }

    private static OperatorPOJO createOperatorDescriptor(
            StreamConfig operatorConfig,
            ClassLoader userCodeClassloader,
            List<OperatorPOJO> operators,
            boolean isHead) {

        OperatorPOJO operatorDescriptor = createNonChainedOperatorDescriptor(
                operatorConfig.getStreamOperatorFactory(userCodeClassloader), userCodeClassloader,
                operatorConfig);
        return operatorDescriptor;
    }

    private static OperatorPOJO createNonChainedOperatorDescriptor(
            StreamOperatorFactory operatorFactory, ClassLoader userCodeClassloader,
            StreamConfig operatorConfig) {
        OperatorPOJO opDesc = new OperatorPOJO();
        LOG.info("operatorFactory class name {}  and getStreamOperatorClass {}", operatorFactory.getClass().getCanonicalName(),
                operatorFactory.getStreamOperatorClass(userCodeClassloader).getCanonicalName());
        fillBasicOperatorDescriptions(opDesc, operatorFactory, operatorConfig, userCodeClassloader);
        if (operatorFactory instanceof org.apache.flink.streaming.api.operators.SimpleInputFormatOperatorFactory) {
            LOG.info("operatorFactory is {}", operatorFactory.getClass().getCanonicalName());
        } else if (operatorFactory instanceof org.apache.flink.streaming.api.operators.SourceOperatorFactory) {
            LOG.info("operatorFactory is {}", operatorFactory.getClass().getCanonicalName());
            SourceOperatorFactory<?> sourceOperatorFactory = operatorConfig.getStreamOperatorFactory(userCodeClassloader);
            Source source = ReflectionUtils.retrievePrivateField(sourceOperatorFactory, "source");
            String sourceName = source.getClass().getSimpleName();
            if (sourceName.equals("NexmarkSource")) {
                LOG.info("parse nexmarkSourceFunction Plan for nexmark 0.3");
                NexmarkFormatPOJO nexmarkFormatPOJO = new NexmarkFormatPOJO();
                nexmarkFormatPOJO.setFormat("nexmark");
                nexmarkFormatPOJO.setBatchSize(10000);
                Map<String, Object> configMap = nexmarkFormatPOJO.getConfigMap();
                try {
                    getNexmarkConfig(source, configMap);
                } catch (Exception exception) {
                    LOG.warn("reflection NexmarkSourceFunction failed!", exception);
                }
                String description = JsonHelper.toJson(nexmarkFormatPOJO);
                opDesc.setDescription(description);
            } else {
                FileSource fileSource = ReflectionUtils.retrievePrivateField(sourceOperatorFactory, "source");

                StreamFormatAdapter streamFormatAdapter = ReflectionUtils.retrievePrivateField(fileSource, "readerFormat");
                StreamFormat streamFormat = ReflectionUtils.retrievePrivateField(streamFormatAdapter, "streamFormat");
                if (streamFormat instanceof CsvReaderFormat) {
                    org.apache.flink.core.fs.Path[] inputPaths = ReflectionUtils.retrievePrivateField(fileSource, "inputPaths");
                    InternalTypeInfo typeInformation = (InternalTypeInfo) fileSource.getProducedType();
                    RowType rowType = typeInformation.toRowType();
                    List<RowType.RowField> rowFields = rowType.getFields();

                    CSVFormatPOJO csvFormatPOJO = new CSVFormatPOJO();
                    SerializableFunction schemaGenerator = ReflectionUtils.retrievePrivateField(streamFormat, "schemaGenerator");
                    SerializableSupplier mapperFactory = ReflectionUtils.retrievePrivateField(streamFormat, "mapperFactory");
                    CsvSchema csvSchema = (CsvSchema) schemaGenerator.apply(mapperFactory.get());
                    CsvSchema.Column[] columns = ReflectionUtils.retrievePrivateField(csvSchema, "_columns");
                    Map<String, CsvSchema.Column> columnsByName = ReflectionUtils.retrievePrivateField(csvSchema, "_columnsByName");
                    int[] csvSelectFieldToProjectFieldMapping = new int[rowFields.size()];

                    int[] csvSelectFieldToCsvFieldMapping = new int[rowFields.size()];
                    List<TypeDescriptionPOJO> fields = new ArrayList<>(columns.length);
                    for (CsvSchema.Column column : columns) {
                        fields.add(new TypeDescriptionPOJO(
                                "logical",
                                true,
                                3, // precision is not available in DataType
                                column.getType().toString(),
                                0, // timestampKind is not available in DataType
                                column.getName()));
                    }

                    for (int i = 0; i < rowFields.size(); i++) {
                        int index = columnsByName.get(rowFields.get(i).getName()).getIndex();
                        csvSelectFieldToCsvFieldMapping[i] = index;
                        fields.get(index).setIsNull(rowFields.get(i).getType().isNullable());
                        fields.get(index).setType(rowFields.get(i).getType().toString());
                        csvSelectFieldToProjectFieldMapping[i] = i;
                    }

                    csvFormatPOJO.setFields(fields);
                    csvFormatPOJO.setFormat("csv");
                    csvFormatPOJO.setFilePath(inputPaths[0].toString());
                    csvFormatPOJO.setSelectFields(csvSelectFieldToProjectFieldMapping);
                    csvFormatPOJO.setCsvSelectFieldToProjectFieldMapping(csvSelectFieldToProjectFieldMapping);
                    csvFormatPOJO.setCsvSelectFieldToCsvFieldMapping(csvSelectFieldToCsvFieldMapping);
                    // csv filed type
                    String description = JsonHelper.toJson(csvFormatPOJO);
                    LOG.info("csvInputFormat original json string {}", description);
                    opDesc.setDescription(description);
                }
            }
        }
        else if (operatorFactory instanceof org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory
                && !(operatorFactory instanceof org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory)) {
            LOG.info("parse nexmarkSourceFunction Plan for nexmark 0.2");
            StreamOperator op = operatorConfig.getStreamOperator(userCodeClassloader);
            if (!(op instanceof StreamSource)) {
                return opDesc;
            }
            StreamSource opSource = (StreamSource) op;
            Function userFunction = opSource.getUserFunction();
            String functionName = userFunction == null ? "NexmarkSourceFunction" : userFunction.getClass().getSimpleName();
            if (functionName.equals("NexmarkSourceFunction")) {
                NexmarkFormatPOJO nexmarkFormatPOJO = new NexmarkFormatPOJO();
                nexmarkFormatPOJO.setFormat("nexmark");
                nexmarkFormatPOJO.setBatchSize(10000);
                Map<String, Object> configMap = nexmarkFormatPOJO.getConfigMap();
                try {
                    getNexmarkConfigForVersion2(userFunction, configMap);
                } catch (Exception exception) {
                    LOG.warn("reflection NexmarkSourceFunction failed!", exception);
                }
                String description = JsonHelper.toJson(nexmarkFormatPOJO);
                opDesc.setDescription(description);
            }
        }
        return opDesc;
    }

    private static void getNexmarkConfig(Source source, Map<String, Object> configMap) throws IllegalAccessException {
        Class<?> nexmarkFunction = source.getClass();
        Field[] fields = nexmarkFunction.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            // first Reflection to get config
            if (!name.equals("config")) {
                continue;
            }
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
            Object value = field.get(source);

            Class<?> config = value.getClass();
            Field[] configFields = config.getDeclaredFields();
            for (Field configField : configFields) {
                String configFieldName = configField.getName();
                if (configFieldName.equals("baseTime") || configFieldName.equals("firstEventId")
                        || configFieldName.equals("maxEvents") || configFieldName.equals("firstEventNumber")) {
                    configMap.put(configFieldName, configField.get(value));
                }
                // second Reflection to get configuration
                if (!configFieldName.equals("configuration")) {
                    continue;
                }
                if (!configField.isAccessible()) {
                    configField.setAccessible(true);
                }
                Object nexmarkConfigValue = configField.get(value);
                // third Reflection to get nexmarkConfig
                Class<?> nexmarkConfig = nexmarkConfigValue.getClass();
                Field[] nexmarkConfigFields = nexmarkConfig.getDeclaredFields();
                for (Field nexmarkConfigField : nexmarkConfigFields) {
                    String nexmarkConfigFieldName = nexmarkConfigField.getName();
                    if (nexmarkConfigFieldName.equals("rateShape") || nexmarkConfigFieldName.equals("rateUnit")) {
                        continue;
                    }
                    if (!configField.isAccessible()) {
                        configField.setAccessible(true);
                    }
                    configMap.put(nexmarkConfigFieldName, nexmarkConfigField.get(nexmarkConfigValue));
                }
            }
            break;
        }
    }

    // parse config for nexmark 0.2, for test, will delete later
    private static void getNexmarkConfigForVersion2(Function userFunction, Map<String, Object> configMap) throws IllegalAccessException {
        Class<?> nexmarkFunction = userFunction.getClass();
        Field[] fields = nexmarkFunction.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            // first Reflection to get config
            if (!name.equals("config")) {
                continue;
            }
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
            Object value = field.get(userFunction);

            Class<?> config = value.getClass();
            Field[] configFields = config.getDeclaredFields();
            for (Field configField : configFields) {
                String configFieldName = configField.getName();
                if (configFieldName.equals("baseTime") || configFieldName.equals("firstEventId")
                        || configFieldName.equals("maxEvents") || configFieldName.equals("firstEventNumber")) {
                    configMap.put(configFieldName, configField.get(value));
                }
                // second Reflection to get configuration
                if (!configFieldName.equals("configuration")) {
                    continue;
                }
                if (!configField.isAccessible()) {
                    configField.setAccessible(true);
                }
                Object nexmarkConfigValue = configField.get(value);
                // third Reflection to get nexmarkConfig
                Class<?> nexmarkConfig = nexmarkConfigValue.getClass();
                Field[] nexmarkConfigFields = nexmarkConfig.getDeclaredFields();
                for (Field nexmarkConfigField : nexmarkConfigFields) {
                    String nexmarkConfigFieldName = nexmarkConfigField.getName();
                    if (nexmarkConfigFieldName.equals("rateShape") || nexmarkConfigFieldName.equals("rateUnit")) {
                        continue;
                    }
                    if (!configField.isAccessible()) {
                        configField.setAccessible(true);
                    }
                    configMap.put(nexmarkConfigFieldName, nexmarkConfigField.get(nexmarkConfigValue));
                }
            }
            break;
        }
    }

    private static void fillBasicOperatorDescriptions(OperatorPOJO opDesc, StreamOperatorFactory operatorFactory, StreamConfig opConfig, ClassLoader userCodeClassloader) {
        opDesc.setName(opConfig.getOperatorName());
        String id = operatorFactory.getStreamOperatorClass(userCodeClassloader).getCanonicalName().split("\\$")[0];
        opDesc.setId(id);
        opDesc.setJobType(opConfig.getJobType());
        opDesc.setTaskType(opConfig.getTaskType());
        opDesc.setOperatorType(opConfig.getOperatorType());
        LOG.info("handling input for op {}", opConfig.getOperatorName());
        LOG.info("handling description for op {}", opConfig.getDescription());
        { // input
            StreamConfig.InputConfig[] inputs = opConfig.getInputs(userCodeClassloader);
            List<TypeDescriptionPOJO> inputsData = new ArrayList<>();

            for (StreamConfig.InputConfig i : inputs) {
                checkState(i instanceof StreamConfig.NetworkInputConfig, "i is not StreamConfig.NetworkInputConfig");
                String[] typeDesc = parseTypeSerializer(((StreamConfig.NetworkInputConfig) i).getTypeSerializer());
                TypeDescriptionPOJO inputType = new TypeDescriptionPOJO();
                inputType.setKind(typeDesc[0]);
                inputType.setType(typeDesc[1]);
                inputsData.add(inputType);
                opDesc.setInputs(inputsData);
            }
        }
        LOG.info("handling descirption and output for op {}", opConfig.getOperatorName());
        { //  description and output
            if (!Objects.equals(id, "org.apache.flink.table.runtime.operators.sink.SinkOperator")) {
                String description = rewriteToOmniDatatypes(opConfig.getDescription()).toString();
                LOG.info("description is {}", description);
                opDesc.setDescription(description);
                String[] typeDesc = parseTypeSerializer(opConfig.getTypeSerializerOut(userCodeClassloader));
                TypeDescriptionPOJO outputType = new TypeDescriptionPOJO();
                outputType.setKind(typeDesc[0]);
                outputType.setType(typeDesc[1]);
                opDesc.setOutput(outputType);

            } else {
                // Placeholder for empty descriptors and output types
                String description = rewriteToOmniDatatypes(opConfig.getDescription()).toString();
                opDesc.setDescription(description);
                TypeDescriptionPOJO outputType = new TypeDescriptionPOJO();
                outputType.setKind("Row");
                outputType.setType(new JSONArray().toString());
                opDesc.setOutput(outputType);
            }
        }
    }

    private static String[] parseTypeSerializer(TypeSerializer<?> ts) {
        String data[] = new String[2];
        String typeObj = "Void";
        String kindStr = "basic";

        // NOTE(yar): Could be enums, but will require syncing enums between Java and C++
        //   The whole thing could be a protobuf instead too
        if (ts instanceof StringSerializer) {
            typeObj = "String";
        } else if (ts instanceof BooleanSerializer) {
            typeObj = "Boolean";
        } else if (ts instanceof ByteSerializer) {
            typeObj = "Byte";
        } else if (ts instanceof CharSerializer) {
            typeObj = "Character";
        } else if (ts instanceof DoubleSerializer) {
            typeObj = "Double";
        } else if (ts instanceof FloatSerializer) {
            typeObj = "Float";
        } else if (ts instanceof LongSerializer) {
            typeObj = "Long";
        } else if (ts instanceof IntSerializer) {
            typeObj = "Short";
        } else if (ts instanceof ShortSerializer) {
            typeObj = "Integer";
        } else if (ts instanceof TupleSerializer) {
            kindStr = "Tuple";
            typeObj = parseTupleSerializer((TupleSerializer<?>) ts).toString();
        } else if (ts instanceof RowDataSerializer) {
            kindStr = "Row";
            try {
                typeObj = extractRowDataSerializer((RowDataSerializer) ts).toString();
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        } else if (ts != null) {
            throw new RuntimeException("Cannot setup OmniOperatorChain because an unsupported serializer type was requested: " + ts.toString());
        }

        data[0] = kindStr; // "kind"
        data[1] = typeObj; // "type"
        return data;
    }

    private static JSONArray parseTupleSerializer(TupleSerializer<?> ts) {
        JSONArray tupleData = new JSONArray();

        for (int i = 0; i < ts.getArity(); i++) {
            tupleData.put(parseTypeSerializer(ts.getFieldSerializers()[i]));
        }

        return tupleData;
    }


    private static JSONArray extractRowDataSerializer(RowDataSerializer rowDataSerializers) throws NoSuchFieldException, IllegalAccessException {
        JSONArray rowFields = new JSONArray();
        Field f = rowDataSerializers.getClass().getDeclaredField("types");
        f.setAccessible(true);
        LogicalType[] logicalTypes = (LogicalType[]) f.get(rowDataSerializers);
        for (LogicalType type : logicalTypes) {
            if (type instanceof BigIntType) {
                rowFields.put(LogicalTypeDescriptor.createJSONDescriptorBigIntType((BigIntType) type));
            } else if (type instanceof VarCharType) {
                rowFields.put(LogicalTypeDescriptor.createJSONDescriptorVarCharType((VarCharType) type));
            } else if (type instanceof TimestampType) {
                rowFields.put(LogicalTypeDescriptor.createJSONDescriptorTimestampType((TimestampType) type));
            } else {

            }

        }
        return rowFields;
    }

    private static JSONObject rewriteToOmniDatatypes(String description) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject = new JSONObject(description);
        } catch (JSONException e) {
            LOG.error("Description is not JSON format {}", description, e);
        }
        return jsonObject;
    }

}
