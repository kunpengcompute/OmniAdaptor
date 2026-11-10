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

package com.huawei.omniruntime.flink.streaming.api.graph;

import static org.apache.flink.util.Preconditions.checkState;

import com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.OperatorDescriptorHelper;
import com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy.AbstractValidateOperatorStrategy;
import com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy.ValidateCalcOPStrategy;
import com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy.ValidateOperatorStrategyFactory;
import com.huawei.omniruntime.flink.utils.DescriptionUtil;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.SimpleInputFormatOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.table.runtime.operators.sink.OutputConversionOperator;
import org.apache.flink.table.runtime.operators.source.InputConversionOperator;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class OmniGraphOverride {
    private static final Logger LOG = LoggerFactory.getLogger(OmniGraphOverride.class);
    private static final Pattern SOURCE_REGEX = Pattern.compile("^Source");
    private static final Pattern CSV_LTZ_TYPE_PATTERN =
            Pattern.compile("^TIMESTAMP_WITH_LOCAL_TIME_ZONE\\((\\d+)\\)$");

    // pattern to extract substring till the first "(" like   "Calc(..."   flink 1.14
    private static final Pattern OPERATOR_NAME_REGEX = Pattern.compile("^\\s*(.*?)[\\[(]");

    // watermark op flink 1.16 name like WatermarkAssigner( ...
    private static final Pattern WATERMARK_OP_NAME_REGEX = Pattern.compile("^\\s*(.*?)\\(");

    private static final Set<String> SUPPORT_STREAM_OP_NAME = new HashSet<>(Arrays.asList(
            "StreamFilter",
            "StreamMap",
            "StreamGroupedReduceOperator",
            "StreamFlatMap",
            "StreamSource",
            "KeyedCoProcessOperator",
            "ProcessOperator"));

    private static final Set<String> SUPPORT_STREAM_TRANSFER_SERIALIZER = new HashSet<>(Arrays.asList(
            "StringSerializer",
            "DoubleSerializer",
            "LongSerializer",
            "BigIntSerializer"));


    private static JobType jobType = JobType.NULL;
    private static List<TaskType> taskTypes = new LinkedList<>();
    private static List<Map<Integer, OperatorType>> operatorTypes = new LinkedList<>();

    private static boolean supportTaskFallback = true;


    private static final Set<String> SUPPORT_OP_NAME = new HashSet<>();
    private static final Set<String> OP_NAME_OF_SQL = new HashSet<>();
    private static final Set<String> VALID_PARTITION_NAMES = new HashSet<>(
            Arrays.asList("ForwardPartitioner", "KeyGroupStreamPartitioner", "RescalePartitioner", "RebalancePartitioner",
                    "GlobalPartitioner"));
    private static final Set<String> WINDOW_OP_NAMES = new HashSet<>(Arrays.asList(
            "WindowAggregate",
            "WindowJoin",
            "GroupWindowAggregate",
            "GlobalWindowAggregate",
            "LocalWindowAggregate"));

    private static final String WRITER_NAME = "Writer";
    private static final String SINK_NAME = "Sink:";

    private static final String[] WATERMARK = {"watermarks", "timestamps"};
    private static final String SINK_REGEX_PATTERN = ".*"
            + Pattern.quote(WRITER_NAME) + "\\s*$" + "|^" + SINK_NAME + ".*";
    private static final Pattern SINK_REGEX = Pattern.compile(SINK_REGEX_PATTERN);

    // native 的 CAST(... AS TIMESTAMP_LTZ) 目前是恒等变换（不做会话时区→UTC 换算），
    // 会话时区非 UTC 时输出与 stock Flink 不同；native 表达式层修复后置 false。
    private static final boolean FALLBACK_ON_ZONE_SENSITIVE_LTZ_CAST = false;

    private static final Set<String> SUPPORT_KAFKA_SCHEMA_TYPE = new HashSet<>();

    private static boolean performanceMode = true;

    private static boolean DATASTREAM_LOW_PARA_MODE = false;

    static {
        try {
            String flinkPerformance = System.getProperty("FLINK_PERFORMANCE");
            if (flinkPerformance != null) {
                LOG.info("flinkPerformance from JVM property is {}", flinkPerformance);
                performanceMode = "true".equals(flinkPerformance);
            } else {
                Map<String, String> envMap = System.getenv();
                if (!CollectionUtil.isNullOrEmpty(envMap) && envMap.containsKey("FLINK_PERFORMANCE")) {
                    flinkPerformance = envMap.getOrDefault("FLINK_PERFORMANCE", "true");
                    LOG.info("flinkPerformance is {}", flinkPerformance);
                    performanceMode = "true".equals(flinkPerformance);
                }
            }
        } catch (Exception exception) {
            LOG.warn("get env failed! ", exception);
        }
        if (performanceMode) {
            // performance mode supports all op temporarily.Some op will be proposed in the future.
            SUPPORT_OP_NAME.addAll(Arrays.asList(
                    "Calc",
                    "Correlate",
                    "LookupJoin",
                    "WatermarkAssigner",
                    "StreamRecordTimestampInserter",
                    "ConstraintEnforcer",
                    "GroupAggregate",
                    "Deduplicate",
                    "Rank"));
        } else {
            SUPPORT_OP_NAME.addAll(Arrays.asList(
                    "Calc",
                    "Correlate",
                    "GroupAggregate",
                    "LocalGroupAggregate",
                    "GlobalGroupAggregate",
                    "IncrementalGroupAggregate",
                    "Join",
                    "LookupJoin",
                    "WindowAggregate",
                    "WindowJoin",
                    "GroupWindowAggregate",
                    "Deduplicate",
                    "Expand",
                    "GlobalWindowAggregate",
                    "LocalWindowAggregate",
                    "WatermarkAssigner",
                    "Rank",
                    "StreamRecordTimestampInserter",
                    "ConstraintEnforcer"));
        }
        SUPPORT_KAFKA_SCHEMA_TYPE.add("JsonRowDataDeserializationSchema");
        OP_NAME_OF_SQL.addAll(Arrays.asList("Calc", "Correlate", "GroupAggregate", "LocalGroupAggregate", "GlobalGroupAggregate",
                "IncrementalGroupAggregate", "Join", "LookupJoin", "WindowAggregate", "WindowJoin", "GroupWindowAggregate",
                "Deduplicate", "Expand", "GlobalWindowAggregate", "LocalWindowAggregate", "WatermarkAssigner", "Rank",
                "StreamRecordTimestampInserter", "ConstraintEnforcer"));
    }

    private static boolean isSourceSupportNative = true;

    private static boolean isSinkSupportNative = true;

    // 当前被校验节点是否为 filesystem+csv 的 file sink。只有这类算子允许绕过文件门禁
    // 走 native；由 validateNodeForOmniTask 按节点计算，供随后调用的 validateSink 读取。
    private static boolean isFileSinkCsvNative = false;
    // 作业里是否存在 filesystem+csv 的 file sink。没有 csv sink 时 filesystem 准入保持
    // master 的行为；出现 csv sink 时才按其能力收紧判定。
    private static boolean jobHasCsvFileSink = false;

    private static boolean isConstraintEnforcerSupportNative = true;

    private static boolean isStreamRecordTimestampInserterSupportNative = true;

    private static final Set<String> SOURCE_SUPPORT_DATA_TYPE = new HashSet<>(Arrays.asList(
            "BIGINT",
            "INTEGER",
            "TIMESTAMP_WITHOUT_TIME_ZONE(0)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(1)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(2)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(9)",
            "VARCHAR(2147483647)",
            "VARCHAR(2000)",
            "VARCHAR(9)",
            "VARCHAR",
            "STRING",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(1)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(2)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)"));

    private static final Set<String> SINK_SUPPORT_DATA_TYPE = new HashSet<>(Arrays.asList(
            "BIGINT",
            "BOOLEAN",
            "INTEGER",
            "TIMESTAMP_WITHOUT_TIME_ZONE(0)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(1)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(2)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(9)",
            "VARCHAR(2147483647)",
            "VARCHAR(2000)",
            "VARCHAR(9)",
            "STRING",
            "DECIMAL64",
            "DECIMAL128",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)"));

    /**
     * setStateBackend
     *
     * @param stateBackend backend
     */
    public static void setStateBackend(StateBackend stateBackend) {
        if (performanceMode && stateBackend != null && !(stateBackend instanceof HashMapStateBackend)) {
            SUPPORT_OP_NAME.clear();
            SUPPORT_OP_NAME.addAll(
                    Arrays.asList("Calc", "Correlate", "WatermarkAssigner", "StreamRecordTimestampInserter",
                            "ConstraintEnforcer",
                            "GroupAggregate", "Deduplicate", "Rank"));
        }
    }


    public static void setNext() {
        if (performanceMode) {
            SUPPORT_OP_NAME.clear();
            SUPPORT_OP_NAME.addAll(
                    Arrays.asList("Calc", "Correlate", "WatermarkAssigner", "StreamRecordTimestampInserter",
                            "ConstraintEnforcer",
                            "GroupAggregate", "Deduplicate", "Rank"));
        }
    }

    /**
     * validateVertexForOmniTask
     *
     * @param vertexEntry   vertexEntry
     * @param chainInfos    chainInfos
     * @param vertexConfigs vertexConfigs
     * @return validate result
     */
    public static boolean validateVertexForOmniTask(Map.Entry<Integer, JobVertex> vertexEntry,
                                                    Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos,
                                                    Map<Integer, Map<Integer, StreamConfig>> chainedConfigs,
                                                    Map<Integer, StreamConfig> vertexConfigs,
                                                    JobType jobType) {

        return validateVertexForOmniTask(vertexEntry, chainInfos, chainedConfigs, vertexConfigs,
                jobType, new HashSet<>());
    }

    public static boolean validateVertexForOmniTask(Map.Entry<Integer, JobVertex> vertexEntry,
            Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos,
            Map<Integer, Map<Integer, StreamConfig>> chainedConfigs,
            Map<Integer, StreamConfig> vertexConfigs, JobType jobType,
            Set<Integer> endpointFallbackVertices) {

        StreamConfig vertexConfig = new StreamConfig(vertexEntry.getValue().getConfiguration());
        Integer vertexID = vertexEntry.getKey();
        LOG.info("validateVertexForOmniTask : vertexID is {}, and vertexName {}",
                vertexID, vertexConfig.getOperatorName());

        JobVertex jobVertex = vertexEntry.getValue();
        refreshJobCsvFileSinkFlag(chainInfos);

        Set<Integer> sinkFallbackNodes = collectSinkFallbackNodes(chainInfos);
        endpointFallbackVertices.remove(vertexID);
        if (validateVertexChainInfoForOmniTask(vertexID, chainInfos, chainedConfigs, jobVertex, vertexConfigs,
                jobType, endpointFallbackVertices, sinkFallbackNodes)) {
            LOG.info("validateVertexForOmniTask  true for : vertexID is {}, and vertexName {}", vertexID, vertexConfig.getOperatorName());
            vertexConfig.setUseOmniEnabled(true);
            return true;
        } else {
            LOG.info("validateVertexForOmniTask  false  for : vertexID is {}, and vertexName {}", vertexID, vertexConfig.getOperatorName());
            vertexConfig.setUseOmniEnabled(false);
            return false;
        }
    }

    public static boolean checkSplitWatermark(Map.Entry<Integer, JobVertex> vertexEntry,
                                           Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos,
                                           boolean otherChainDoSplit) {
        StreamConfig vertexConfig = new StreamConfig(vertexEntry.getValue().getConfiguration());
        boolean nowChainDoSplit = false;
        for (Integer key : chainInfos.keySet()) {
            StreamingJobGraphGenerator.OperatorChainInfo chainInfo = chainInfos.get(key);
            List<StreamNode> streamNodes = chainInfo.getAllChainedNodes();
            for (StreamNode node : streamNodes) {
                nowChainDoSplit = nowChainDoSplit || node.getOperatorName().contains("Window");
            }
            vertexConfig.setSplitWatermark(otherChainDoSplit || nowChainDoSplit);
        }
        return vertexConfig.isSplitWatermark();
    }

    private static boolean checkDataStreamSupportTransferSerializer(TypeSerializer<?> typeSerializer) {
        if (typeSerializer == null) {
            return false;
        }
        if (typeSerializer instanceof TupleSerializer) {
            TypeSerializer<Object>[] fieldSerializers = ((TupleSerializer<?>) typeSerializer).getFieldSerializers();
            for (TypeSerializer<Object> fieldSerializer : fieldSerializers) {
                if (!checkDataStreamSupportTransferSerializer(fieldSerializer)) {
                    return false;
                }
            }
            return true;
        }
        if (typeSerializer instanceof ListSerializer) {
            TypeSerializer<?> elementSerializer = ReflectionUtils.retrievePrivateField(typeSerializer, "elementSerializer");
            return checkDataStreamSupportTransferSerializer(elementSerializer);
        }
        if (typeSerializer instanceof MapSerializer) {
            TypeSerializer<?> keySerializer = ((MapSerializer<?, ?>) typeSerializer).getKeySerializer();
            TypeSerializer<?> valueSerializer = ((MapSerializer<?, ?>) typeSerializer).getValueSerializer();
            return checkDataStreamSupportTransferSerializer(keySerializer)
                    && checkDataStreamSupportTransferSerializer(valueSerializer);
        }
        if (typeSerializer instanceof PojoSerializer) {
            TypeSerializer<Object>[] fieldSerializers = ReflectionUtils.retrievePrivateField(typeSerializer, "fieldSerializers");
            for (TypeSerializer<?> fieldSerializer : fieldSerializers) {
                if (!checkDataStreamSupportTransferSerializer(fieldSerializer)) {
                    return false;
                }
            }
            return true;
        }
        return SUPPORT_STREAM_TRANSFER_SERIALIZER.contains(typeSerializer.getClass().getSimpleName())
                || typeSerializer.getClass().getName().contains(
                "org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo");
    }

    private static boolean validateVertexChainInfoForOmniTask(Integer vertexID,
                                                              Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos,
                                                              Map<Integer, Map<Integer, StreamConfig>> chainedConfigs, JobVertex jobVertex,
                                                              Map<Integer, StreamConfig> vertexConfigs, JobType jobType,
                                                              Set<Integer> endpointFallbackVertices,
                                                              Set<Integer> sinkFallbackNodes) {
        // walkthrough each operator
        StreamingJobGraphGenerator.OperatorChainInfo chainInfo = chainInfos.get(vertexID);
        if (chainInfo == null) {
            LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID {} is null", vertexID);
            return false;
        }

        StreamGraph streamGraph = chainInfo.getStreamGraph();
        List<StreamNode> chainedNode = chainInfo.getAllChainedNodes();
        boolean unsupported = false;
        boolean endpointOnly = true;
        for (StreamNode node : chainedNode) {
            boolean sinkFallback = sinkFallbackNodes.contains(node.getId());
            if (sinkFallback || validateNodeForOmniTask(vertexID, vertexConfigs, jobType, streamGraph, node)) {
                unsupported = true;
                endpointOnly &= sinkFallback || isFallbackEndpoint(node);
                if (sinkFallback) {
                    LOG.info("Sink-stage fallback: vertex={}, operator={}", vertexID, node.getOperatorName());
                }
            }
        }

        if (unsupported) {
            if (endpointOnly) {
                endpointFallbackVertices.add(vertexID);
            }
            LOG.info("Task fallback: vertex={}, endpointOnly={}", vertexID, endpointOnly);
            return false;
        }

        if (FALLBACK_ON_ZONE_SENSITIVE_LTZ_CAST && chainHasZoneSensitiveLtzCast(chainedNode)) {
            String sessionTimeZone = chainSessionTimeZone(chainedNode, streamGraph);
            if (!isUtcTimeZone(sessionTimeZone)) {
                LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID {} falls back to stock Flink: "
                        + "the chain casts between TIMESTAMP and TIMESTAMP_LTZ while the session time "
                        + "zone is {} and the native cast does not convert to UTC", vertexID, sessionTimeZone);
                return false;
            }
        }

        StreamNode lastNode = chainedNode.get(0);
        StreamNode firstNode = chainedNode.get(chainedNode.size() - 1);

        if (jobType == JobType.STREAM
                && "Map".equals(lastNode.getOperatorName())
                && firstNode.getOperatorFactory() instanceof SourceOperatorFactory) {
            SourceOperatorFactory<?> sourceOperatorFactory = (SourceOperatorFactory<?>) firstNode.getOperatorFactory();
            Source source = ReflectionUtils.retrievePrivateField(sourceOperatorFactory, "source");
            if ("KafkaSource".equals(source.getClass().getSimpleName())) {
                LOG.info("unsupported operators kafkaSource and map for this job in native, roll back.");
                return false;
            }
        }

        // Validates the serializer for DataStream transmission.
        if (validateDataStreamSerializer(jobType, chainedNode)) {
            return false;
        }

        // set jobType, taskType, operatorType for each StreamConfig
        for (int i = 0; i < operatorTypes.size(); i++) {
            for (Map.Entry<Integer, OperatorType> entry : operatorTypes.get(i).entrySet()) {
                Integer Id = entry.getKey();
                OperatorType operatorType = entry.getValue();
                TaskType taskType = taskTypes.get(i);

                vertexConfigs.get(Id).setJobType(jobType.getValue());
                vertexConfigs.get(Id).setTaskType(taskType.getValue());
                vertexConfigs.get(Id).setOperatorType(operatorType.getValue());
            }
        }

        if (jobType.equals(JobType.STREAM) || jobType.equals(JobType.SQL)) {
            StreamConfig config = vertexConfigs.get(vertexID); // the StreamConfig of the first operator in the operatorChain/task.
            config.setTransitiveChainedTaskConfigsOptimized(chainedConfigs.get(vertexID));
        }

        LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID  {} is {}", vertexID, true);
        return true;
    }

    private static boolean validateDataStreamSerializer(JobType jobType, List<StreamNode> chainedNode) {
        if (jobType != JobType.STREAM) {
            return false;
        }
        StreamNode firstNode = chainedNode.get(0);
        StreamNode lastNode = chainedNode.get(chainedNode.size() - 1);

        TypeSerializer<?> stateKeySerializer = firstNode.getStateKeySerializer();

        if (stateKeySerializer != null && !checkDataStreamSupportTransferSerializer(stateKeySerializer)) {
            LOG.info("unsupported serializer for State Key " +
                    "in typeSerializersIn of {}", firstNode.getOperatorName());
            return true;
        }

        TypeSerializer<?>[] typeSerializersIn = firstNode.getTypeSerializersIn();
        TypeSerializer<?> typeSerializerOut = lastNode.getTypeSerializerOut();
        if (typeSerializersIn != null) {
            for (TypeSerializer<?> typeSerializer : typeSerializersIn) {
                if (!checkDataStreamSupportTransferSerializer(typeSerializer)) {
                    LOG.info("unsupported serializer for DataStream transmission " +
                            "in typeSerializersIn of {}", firstNode.getOperatorName());
                    return true;
                }
            }
        }
        if (typeSerializerOut != null && !checkDataStreamSupportTransferSerializer(typeSerializerOut)) {
            LOG.info("unsupported serializer for DataStream transmission " +
                    "in typeSerializerOut of {}", lastNode.getOperatorName());
            return true;
        }
        return false;
    }

    /**
     * 判断链内是否存在跨 TIMESTAMP_LTZ 边界的 CAST。看 originDescription 文本而非类型表：
     * 反向 CAST(ltz AS TIMESTAMP) 的类型表里没有 LTZ；不带 CAST( 的 LTZ 表达式
     * （TO_TIMESTAMP_LTZ / DATE_FORMAT）native 已按 vanilla 语义实现，需排除。
     */
    private static boolean chainHasZoneSensitiveLtzCast(List<StreamNode> chainedNodes) {
        ObjectMapper mapper = new ObjectMapper();
        for (StreamNode node : chainedNodes) {
            String description = node.getOperatorDescription();
            if (description == null || !description.startsWith("{")) {
                continue;
            }
            try {
                JsonNode origin = mapper.readTree(description).get("originDescription");
                if (origin == null) {
                    continue;
                }
                String originText = origin.asText();
                if (originText.contains("CAST(")
                        && (originText.contains("TIMESTAMP_LTZ")
                                || originText.contains("TIMESTAMP_WITH_LOCAL_TIME_ZONE"))) {
                    return true;
                }
            } catch (Exception ignored) {
                // 描述不是 JSON（源/sink 的简单名描述）时跳过
            }
        }
        return false;
    }

    /** 描述符里的 timeZone 即作业的 table.local-time-zone（例：GMT+08:00 / UTC）。 */
    private static String chainSessionTimeZone(List<StreamNode> chainedNodes, StreamGraph streamGraph) {
        // Prefer the effective SQL session setting when it is carried by StreamGraph.
        if (streamGraph != null) {
            java.util.Optional<String> configuredTimeZone = streamGraph.getConfiguration().getOptional(
                    org.apache.flink.configuration.ConfigOptions.key("table.local-time-zone")
                            .stringType().noDefaultValue());
            if (configuredTimeZone.isPresent() && !configuredTimeZone.get().trim().isEmpty()) {
                return configuredTimeZone.get();
            }
        }

        ObjectMapper mapper = new ObjectMapper();
        for (StreamNode node : chainedNodes) {
            String description = node.getOperatorDescription();
            if (description == null || !description.contains("\"timeZone\"")) {
                continue;
            }
            try {
                JsonNode timeZone = mapper.readTree(description).get("timeZone");
                if (timeZone != null && !timeZone.asText().isEmpty()) {
                    return timeZone.asText();
                }
            } catch (Exception ignored) {
                // 不是单个节点的 JSON 描述，跳过
            }
        }
        return null;
    }

    private static boolean isUtcTimeZone(String timeZone) {
        if (timeZone == null) {
            return false;
        }
        String normalized = timeZone.trim().toUpperCase(Locale.ROOT);
        return normalized.equals("UTC") || normalized.equals("Z") || normalized.equals("GMT")
                || normalized.equals("GMT+00:00") || normalized.equals("GMT-00:00")
                || normalized.equals("ETC/UTC") || normalized.equals("ETC/GMT");
    }

    private static boolean validateNodeForOmniTask(Integer vertexID, Map<Integer, StreamConfig> vertexConfigs,
                                                   JobType jobType, StreamGraph streamGraph, StreamNode node) {
        String operatorName = node.getOperatorName();
        String operatorDescription = node.getOperatorDescription();
        // 按当前节点判定 filesystem+csv 的 sink 是否允许走 native。
        // checkpoint 开启时 native csv sink 无 pending-file/commit 恢复语义，回退 stock。
        boolean csvSinkCandidate = computeFileSinkCsvNative(node);
        boolean csvSinkAllowed = csvSinkCandidate && !isCheckpointingEnabled(streamGraph);
        if (csvSinkCandidate && !csvSinkAllowed) {
            LOG.info("CSV file sink {} falls back to stock Flink: checkpointing is enabled, "
                    + "native has no recoverable pending-file/commit state", operatorName);
        }
        isFileSinkCsvNative = csvSinkAllowed;
        if (jobHasCsvFileSink && requiresFlinkFileSink(operatorName, node.getOperatorFactory())
                && !isFileSinkCsvNative) {
            LOG.info("File operator {} uses Flink: native file enumeration, restore and recoverable commit "
                    + "are not implemented", operatorName);
            return true;
        }
        switch (jobType) {
            case SQL:
                // check partitioner type
                if (validatePartitioner(vertexID, node, streamGraph, operatorName)) {
                    return true;
                }
                LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID {} "
                                + "has the operator {} with description {}",
                        vertexID, operatorName, operatorDescription);
                if (validateOperatorByNameForOmniTask(operatorName, operatorDescription,
                        node.getOperatorFactory())) {
                    LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID {} "
                            + ": the operator {} is SUITABLE for OmniTask", vertexID, operatorName);
                } else {
                    LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID {} "
                            + ": the operator {} is NOT SUITABLE for OmniTask", vertexID, operatorName);
                    return true;
                }
                break;
            case STREAM:
                StreamConfig streamConfig = vertexConfigs.get(node.getId());
                if (!DATASTREAM_LOW_PARA_MODE) {
                    streamConfig.setLowParaMode(false);
                } else {
                    if (isSink(operatorName)) {
                        return true;
                    }
                }
                boolean result;
                try {
                    result = validateWatermark(node) && StreamNodeOptimized.getInstance().setExtraDescription(
                            node, streamConfig, streamGraph, jobType);
                } catch (NoSuchFieldException | IllegalAccessException | IOException | ClassNotFoundException e) {
                    throw new FlinkRuntimeException(
                            "Error occurs during the process of compatibility between new and old sinks or sources", e);
                }

                if (!result) {
                    LOG.info("setExtraDescription StreamNode of vertex ID  {} "
                            + ": the operator {} is NOT SUITABLE for OmniTask", vertexID, operatorName);
                    return true;
                }
                break;
            default:
                LOG.error("vertex ID  {} Invalid JobType.", vertexID);
                return true;
        }
        return false;
    }

    /**
     * rollback the DataStream Watermark operator, the function of watermark has been implemented by native frameworks.
     *
     * @param streamNode the operator object.
     * @return return the reulst weather the operator should rollback or not.
     *
     *
     */
    private static boolean validateWatermark(StreamNode streamNode) {
        if (streamNode.getOperatorName().toLowerCase(Locale.ROOT).contains(WATERMARK[0])
                || streamNode.getOperatorName().toLowerCase(Locale.ROOT).contains(WATERMARK[1])) {
            LOG.info("stream node contains watermark");
            return false;
        }
        return true;
    }

    private static boolean validatePartitioner(Integer vertexID, StreamNode node,
                                               StreamGraph streamGraph, String operatorName) {
        boolean result;
        for (StreamEdge edge : streamGraph.getStreamEdges(node.getId())) {
            if (edge.getPartitioner() != null) {
                String partitionName = edge.getPartitioner().getClass().getSimpleName();
                LOG.info("Partition strategy for node {}: {}", node.getOperatorName(), partitionName);
                if (!VALID_PARTITION_NAMES.contains(partitionName)) {
                    LOG.info("validatePartitionStrategy for the operation {} is false ", operatorName);
                    result = false;
                    LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID {} is {}",
                            vertexID, result);
                    return true;
                }
                if (supportTaskFallback && (!"ForwardPartitioner".equals(partitionName))) {
                    supportTaskFallback = false;
                }
            } else {
                return true;
            }
        }
        return false;
    }

    public static void generateTypeInfo(Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos) {
        // 1. check if the job includes conversion operator
        if (containsDataStreamToTable(chainInfos) || containsTableToDataStream(chainInfos)) {
            setOperatorType(chainInfos);
            return;
        }
        // 2. check if the job includes table stream operator
        if (containsTableStreamOperator(chainInfos)) {
            setAllOperatorType(chainInfos, OperatorType.SQL);
            return;
        }
        // 3. don't contain table stream operator
        setAllOperatorType(chainInfos, OperatorType.STREAM);
    }

    public static JobType getJobType() {
        return jobType;
    }

    public static boolean isSupportTaskFallback() {
        return supportTaskFallback;
    }

    public static List<TaskType> getTaskTypes() {
        return taskTypes;
    }

    public static List<Map<Integer, OperatorType>> getOperatorTypes() {
        return operatorTypes;
    }

    public static void clearTypeInfo() {
        operatorTypes.clear();
    }

    public static JobType getJobType(Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos) {
        JobType jobType = JobType.NULL;
        Set<Integer> sinkFallbackNodes = collectSinkFallbackNodes(chainInfos);
        boolean onlySourceAndSink = true;
        List<String> inputTypeList = new ArrayList<>();
        for (StreamingJobGraphGenerator.OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamNode> chainedNode = chainInfo.getAllChainedNodes();
            for (StreamNode node : chainedNode) {
                if (sinkFallbackNodes.contains(node.getId())) {
                    continue;
                }
                String operatorName = node.getOperatorName();
                if (isSource(operatorName)) {
                    jobType = getSourceJobType(node, operatorName, jobType, inputTypeList);
                    continue;
                }
                if (isSink(operatorName)) {
                    jobType = getSinkJobType(node, operatorName, jobType);
                    continue;
                }
                if (operatorName.contains(": Committer")) {
                    continue;
                }
                onlySourceAndSink = false;
                String opSimpleName = extractOperatorName(operatorName);
                if (SUPPORT_OP_NAME.contains(opSimpleName)) {
                    jobType = jobType.getCombinationsJobType(JobType.SQL);
                }

                if (node.getOperatorFactory() instanceof SimpleOperatorFactory) {
                    operatorName = node.getOperator().getClass().getSimpleName();
                }

                if (SUPPORT_STREAM_OP_NAME.contains(operatorName)) {
                    jobType = jobType.getCombinationsJobType(JobType.STREAM);
                }
            }
        }
        if (onlySourceAndSink && jobType.equals(JobType.SQL)) {
            ValidateCalcOPStrategy validate = new ValidateCalcOPStrategy();
            if (!inputTypeList.isEmpty() && !validate.validateDataTypes(Collections.singletonList(inputTypeList))) {
                jobType = JobType.NULL;
            }
        }
        return jobType;
    }

    private static JobType getSinkJobType(StreamNode node, String operatorName, JobType jobType) {
        TypeSerializer<?>[] typeSerializersIn = node.getTypeSerializersIn();
        if (typeSerializersIn == null || typeSerializersIn.length == 0) {
            throw new FlinkRuntimeException("Empty type serializer ins for operator " + operatorName);
        }
        JobType newJobType;
        if (typeSerializersIn[0] instanceof AbstractRowDataSerializer) {
            newJobType = jobType.getCombinationsJobType(JobType.SQL);
        } else {
            newJobType = jobType.getCombinationsJobType(JobType.STREAM);
        }
        return newJobType;
    }

    private static boolean isSourceSql(StreamNode node, String operatorName) {
        TypeSerializer<?> typeSerializerOut = node.getTypeSerializerOut();
        if (typeSerializerOut == null) {
            throw new FlinkRuntimeException("Empty type serializer out for operator " + operatorName);
        }
        return typeSerializerOut instanceof AbstractRowDataSerializer;
    }

    private static boolean isSinkSql(StreamNode node, String operatorName) {
        TypeSerializer<?>[] typeSerializersIn = node.getTypeSerializersIn();
        if (typeSerializersIn == null || typeSerializersIn.length == 0) {
            throw new FlinkRuntimeException("Empty type serializer ins for operator " + operatorName);
        }
        return typeSerializersIn[0] instanceof AbstractRowDataSerializer;
    }

    private static JobType getSourceJobType(StreamNode node, String operatorName, JobType jobType,
                                            List<String> inputTypeList) {
        TypeSerializer<?> typeSerializerOut = node.getTypeSerializerOut();
        if (typeSerializerOut == null) {
            throw new FlinkRuntimeException("Empty type serializer out for operator " + operatorName);
        }
        JobType newJobType;
        if (typeSerializerOut instanceof AbstractRowDataSerializer) {
            newJobType = jobType.getCombinationsJobType(JobType.SQL);
            buildInputTypes(inputTypeList, typeSerializerOut);
        } else {
            newJobType = jobType.getCombinationsJobType(JobType.STREAM);
        }
        for (String type : inputTypeList) {
            type = AbstractValidateOperatorStrategy.stripNotNull(type);
            isSourceSupportNative = SOURCE_SUPPORT_DATA_TYPE.contains(type);
            if (!isSourceSupportNative) {
                break;
            }
        }
        return newJobType;
    }

    private static void getInputTypes(StreamNode node) {
        String operatorName = node.getOperatorName();
        List<String> inputTypeList = new ArrayList<>();
        TypeSerializer<?>[] typeSerializersIns = node.getTypeSerializersIn();
        for (TypeSerializer<?> typeSerializersIn : typeSerializersIns) {
            if (typeSerializersIn instanceof AbstractRowDataSerializer) {
                buildInputTypes(inputTypeList, typeSerializersIn);
            }
        }
        boolean isSupportNative = false;
        for (String type : inputTypeList) {
            String originalType = type;
            type = AbstractValidateOperatorStrategy.stripNotNull(type);
            if (type.matches("^DECIMAL64\\([^)]*\\)$")) {
                type = "DECIMAL64";
                LOG.info("converted to DECIMAL64");
            }

            if (type.matches("^DECIMAL128\\([^)]*\\)$")) {
                type = "DECIMAL128";
                LOG.info("converted to DECIMAL128");
            }
            isSupportNative = SINK_SUPPORT_DATA_TYPE.contains(type);
            if (!isSupportNative && isCsvSinkLtzExempt(operatorName, type)) {
                // 通用表只登记了 LTZ(3)，这里按算子名给 csv sink 放行 LTZ(0..2)，不影响
                // 其它 sink。CHAR/VARCHAR 的长度截断语义尚未对齐，仍由通用表拦下回退。
                LOG.info("csv file sink {} admits {} through the csv gate", operatorName, type);
                isSupportNative = true;
            }
            if (!isSupportNative) {
                LOG.info("getInputTypes unsupported sink type for operator '{}': '{}' (original: '{}')",
                        operatorName, type, originalType);
                break;
            }
        }
        if (isSink(operatorName)) {
            isSinkSupportNative = isSupportNative;
        } else if (isConstraintEnforcer(operatorName)) {
            isConstraintEnforcerSupportNative = isSupportNative;
        } else if (isStreamRecordTimestampInserter(operatorName)) {
            isStreamRecordTimestampInserterSupportNative = isSupportNative;
        }
    }

    /** csv filesystem sink 的 TIMESTAMP_LTZ(p<=3) 豁免通用 sink 能力表；绑定 StreamingFileWriter。 */
    private static boolean isCsvSinkLtzExempt(String operatorName, String type) {
        if (operatorName == null || !operatorName.contains("StreamingFileWriter")) {
            return false;
        }
        Matcher matcher = CSV_LTZ_TYPE_PATTERN.matcher(type);
        return matcher.matches() && Integer.parseInt(matcher.group(1)) <= 3;
    }

    private static void buildInputTypes(List<String> inputTypeList, TypeSerializer<?> typeSerializerOut) {
        if (!(typeSerializerOut instanceof RowDataSerializer)) {
            return;
        }
        LogicalType[] types = ReflectionUtils.retrievePrivateField(typeSerializerOut, "types");
        for (LogicalType type : types) {
            if (type.getTypeRoot() == LogicalTypeRoot.ROW) {
                checkState(type instanceof RowType);
                List<RowType.RowField> subFields = ((RowType) type).getFields();
                for (RowType.RowField rowField : subFields) {
                    inputTypeList.add(DescriptionUtil.getFieldType(rowField.getType()));
                }
            } else {
                inputTypeList.add(DescriptionUtil.getFieldType(type));
            }
        }
    }

    // true for omniTask
    private static boolean validateOperatorByNameForOmniTask(String operatorName, String operatorDescription,
                                                             StreamOperatorFactory operatorFactory) {
        if (isSource(operatorName)) {
            return validateSource(operatorDescription, operatorFactory);
        } else if (isSink(operatorName)) {
            return validateSink(operatorName, operatorFactory);
        } else if (isConstraintEnforcer(operatorName)) {
            return validateConstraintEnforcer();
        } else if (isStreamRecordTimestampInserter(operatorName)) {
            return validateStreamRecordTimestampInserter();
        } else if (operatorName.contains(": Committer")) {
            return true;
        } else if (isPartitionCommitter(operatorName)) {
            return true;
        } else {
            String opSimpleName = extractOperatorName(operatorName);

            if (!SUPPORT_OP_NAME.contains(opSimpleName)) {
                LOG.info("The operator name {} is not supported.", opSimpleName);
                return false;
            }
            if (operatorDescription.contains("INVALID")) {
                LOG.info("The operator description contains INVALID operator/expressions. ");
                return false;
            }
            Map<String, Object> jsonMap = toJsonMap(operatorDescription);

            AbstractValidateOperatorStrategy validateStrategy =
                    ValidateOperatorStrategyFactory.getStrategy(opSimpleName);
            return validateStrategy.executeValidateOperator(jsonMap);
        }
    }

    private static boolean validateConstraintEnforcer() {
        return isConstraintEnforcerSupportNative;
    }

    private static boolean validateStreamRecordTimestampInserter() {
        return isStreamRecordTimestampInserterSupportNative;
    }

    static boolean requiresFlinkFileSink(String operatorName, StreamOperatorFactory<?> factory) {
        if (operatorName.contains("StreamingFileWriter") || operatorName.contains("PartitionCommitter")) {
            return true;
        }
        if (factory instanceof SinkWriterOperatorFactory) {
            return ((SinkWriterOperatorFactory<?, ?>) factory).getSink() instanceof FileSink;
        }
        if (factory instanceof CommitterOperatorFactory) {
            try {
                Object sink = ReflectionUtils.retrievePrivateField(factory, "sink");
                return sink instanceof FileSink;
            } catch (RuntimeException e) {
                LOG.debug("Cannot identify committer sink type; do not force file-sink fallback", e);
                return false;
            }
        }
        return false;
    }

    /** checkpoint 是否开启；开启时 native csv sink 无 pending-file/commit 恢复语义，sink 回退 stock。 */
    private static boolean isCheckpointingEnabled(StreamGraph streamGraph) {
        return streamGraph != null && streamGraph.getCheckpointConfig().isCheckpointingEnabled();
    }

    /** 只看 sink 配置（不看行类型/选项）判定是否 filesystem+csv file sink，用于给作业分流。 */
    private static boolean isCsvFileSink(StreamNode node) {
        String operatorName = node.getOperatorName();
        if (operatorName == null || !operatorName.contains("StreamingFileWriter")) {
            return false;
        }
        try {
            StreamOperator<?> operator = node.getOperator();
            if (operator == null) {
                return false;
            }
            Object confObj = ReflectionUtils.retrievePrivateField(operator, "conf");
            return confObj instanceof Configuration
                    && "csv".equals(((Configuration) confObj).getString("format", ""));
        } catch (Exception e) {
            return false;
        }
    }

    private static void refreshJobCsvFileSinkFlag(
            Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos) {
        boolean hasCsvFileSink = false;
        for (StreamingJobGraphGenerator.OperatorChainInfo chainInfo : chainInfos.values()) {
            for (StreamNode node : chainInfo.getAllChainedNodes()) {
                if (isCsvFileSink(node)) {
                    hasCsvFileSink = true;
                    break;
                }
            }
            if (hasCsvFileSink) {
                break;
            }
        }
        jobHasCsvFileSink = hasCsvFileSink;
    }

    /**
     * filesystem+csv 的 StreamingFileWriter 才允许走 native：选项能力与行类型能力都满足
     * 才走 native，任一不满足则整链回退 stock Flink。
     */
    private static boolean computeFileSinkCsvNative(StreamNode node) {
        String operatorName = node.getOperatorName();
        if (operatorName == null || !operatorName.contains("StreamingFileWriter")) {
            return false;
        }
        try {
            StreamOperator<?> operator = node.getOperator();
            if (operator == null) {
                return false;
            }
            Object confObj = ReflectionUtils.retrievePrivateField(operator, "conf");
            if (!(confObj instanceof Configuration)) {
                return false;
            }
            Map<String, String> opts = ((Configuration) confObj).toMap();
            return csvSinkOptionsAllowed(operatorName, opts) && csvSinkRowTypeAllowed(node);
        } catch (Exception e) {
            LOG.warn("Failed to detect filesystem csv sink for {}, defaulting to fallback",
                    operatorName, e);
            return false;
        }
    }

    /**
     * CSV sink 选项门禁：native 无法逐字节复现的配置一律整链回退 stock Flink，
     * 而不是硬失败或静默产出不同字节。
     */
    private static boolean csvSinkOptionsAllowed(String operatorName, Map<String, String> opts) {
        if (!"csv".equals(opts.get("format"))) {
            return false;
        }
        if (Boolean.parseBoolean(opts.getOrDefault("csv.disable-quote-character", "false"))) {
            LOG.info("CSV file sink {} falls back to stock Flink: csv.disable-quote-character is enabled",
                    operatorName);
            return false;
        }
        // auto-compaction 依赖 checkpoint 提交与 .uncompacted-* 前缀，native 无法复现，整体回退。
        if (Boolean.parseBoolean(opts.getOrDefault("auto-compaction", "false"))) {
            LOG.info("CSV file sink {} falls back to stock Flink: auto-compaction is enabled (stock "
                    + "writes .uncompacted-* part files that native does not reproduce)", operatorName);
            return false;
        }
        // ARRAY 列已被嵌套行类型门禁挡住；这里把选项显式化，避免将来放开 ARRAY 时漏掉。
        if (opts.containsKey("csv.array-element-delimiter")) {
            LOG.info("CSV file sink {} falls back to stock Flink: csv.array-element-delimiter is not "
                    + "supported by native CSV encoding", operatorName);
            return false;
        }
        // native 以单个 char 存储分隔符/引号/转义符，多字节 UTF-8 或 NUL 会被截断成
        // 首字节静默产出坏数据，故要求单字节 ASCII（vanilla 本身支持完整字符）。
        String fieldDelimiter = org.apache.commons.lang3.StringEscapeUtils.unescapeJava(
                opts.getOrDefault("csv.field-delimiter", ","));
        if (!isSingleAsciiChar(fieldDelimiter)) {
            LOG.info("CSV file sink {} falls back to stock Flink: csv.field-delimiter '{}' is not a "
                    + "single ASCII character", operatorName, fieldDelimiter);
            return false;
        }
        if (!isSingleAsciiChar(opts.getOrDefault("csv.quote-character", "\""))) {
            LOG.info("CSV file sink {} falls back to stock Flink: csv.quote-character is not a single "
                    + "ASCII character", operatorName);
            return false;
        }
        String escapeCharacter = opts.get("csv.escape-character");
        if (escapeCharacter != null && !isSingleAsciiChar(escapeCharacter)) {
            LOG.info("CSV file sink {} falls back to stock Flink: csv.escape-character is not a single "
                    + "ASCII character", operatorName);
            return false;
        }
        return true;
    }

    /**
     * CSV 行类型门禁：只放行白名单逻辑类型（含 partition 列），嵌套类型、TIMESTAMP(p>3)
     * 与 DOUBLE/DATE/TIME 等一律回退，类型未知时同样回退。
     */
    private static boolean csvSinkRowTypeAllowed(StreamNode node) {
        String operatorName = node.getOperatorName();
        LogicalType[] types = collectInputLogicalTypes(node);
        if (types == null || types.length == 0) {
            LOG.info("CSV file sink {} falls back to stock Flink: input logical types are unknown",
                    operatorName);
            return false;
        }
        for (LogicalType type : types) {
            if (type instanceof ArrayType || type instanceof RowType || type instanceof MapType
                    || type instanceof MultisetType) {
                LOG.info("CSV file sink {} falls back to stock Flink: nested logical type {} is not "
                        + "supported by native CSV encoding", operatorName, type);
                return false;
            }
            if (type instanceof TimestampType) {
                int precision = ((TimestampType) type).getPrecision();
                if (precision > 3) {
                    LOG.info("CSV file sink {} falls back to stock Flink: TIMESTAMP precision {} exceeds "
                            + "native CSV support (0..3)", operatorName, precision);
                    return false;
                }
                continue;
            }
            if (type instanceof LocalZonedTimestampType) {
                int precision = ((LocalZonedTimestampType) type).getPrecision();
                if (precision > 3) {
                    LOG.info("CSV file sink {} falls back to stock Flink: TIMESTAMP_LTZ precision {} exceeds "
                            + "native CSV support (0..3)", operatorName, precision);
                    return false;
                }
                continue;
            }
            if (!(type instanceof BigIntType || type instanceof IntType || type instanceof BooleanType
                    || type instanceof VarCharType || type instanceof CharType
                    || type instanceof DecimalType)) {
                LOG.info("CSV file sink {} falls back to stock Flink: logical type {} is not supported by "
                        + "native CSV encoding", operatorName, type);
                return false;
            }
            // 与 OperatorDescriptorHelper.nativeInputTypeName 保持同步：名字没映射到时
            // native 会少建一列并硬崩，这里提前回退。
            if (OperatorDescriptorHelper.nativeInputTypeName(type) == null) {
                LOG.info("CSV file sink {} falls back to stock Flink: logical type {} has no native "
                        + "inputTypes mapping", operatorName, type);
                return false;
            }
        }
        return csvSinkPartitionKeysAllowed(node);
    }

    /**
     * CSV 分区键门禁：分区目录值由 BucketAssigner 经 VectorBatchUtil::getValueAtAsStr 渲染，
     * 该函数对 TIMESTAMP/DECIMAL/DOUBLE 的输出与 vanilla 不一致，因此只放行渲染结果一致的
     * 列类型，其余回退，避免静默产出不同的目录与文件分布。
     */
    private static boolean csvSinkPartitionKeysAllowed(StreamNode node) {
        String operatorName = node.getOperatorName();
        try {
            StreamOperator<?> operator = node.getOperator();
            if (operator == null) {
                LOG.info("CSV file sink {} falls back to stock Flink: cannot inspect partition keys",
                        operatorName);
                return false;
            }
            Object keysObj = ReflectionUtils.retrievePrivateField(operator, "partitionKeys");
            if (!(keysObj instanceof List) || ((List<?>) keysObj).isEmpty()) {
                // 无分区键：native 分区取值路径不参与，目录值渲染不影响结果。
                return true;
            }
            int[] partitionIndexes = partitionIndexesOf(operator);
            LogicalType[] inputTypes = collectInputLogicalTypes(node);
            if (partitionIndexes == null || inputTypes == null) {
                LOG.info("CSV file sink {} falls back to stock Flink: cannot resolve partition key types "
                        + "(partition keys {})", operatorName, keysObj);
                return false;
            }
            for (int index : partitionIndexes) {
                if (index < 0 || index >= inputTypes.length) {
                    LOG.info("CSV file sink {} falls back to stock Flink: partition key index {} is out of "
                            + "range of the {} input columns", operatorName, index, inputTypes.length);
                    return false;
                }
                LogicalType keyType = inputTypes[index];
                if (!isNativePartitionValueRenderable(keyType)) {
                    LOG.info("CSV file sink {} falls back to stock Flink: the native partition path renders "
                            + "partition key column {} (logical type {}) differently from vanilla Flink",
                            operatorName, index, keyType);
                    return false;
                }
            }
            LOG.info("CSV file sink {} uses the native partition path for partition keys {} (indices {})",
                    operatorName, keysObj, Arrays.toString(partitionIndexes));
            return true;
        } catch (Exception e) {
            LOG.warn("Cannot inspect partition keys of CSV file sink {}, defaulting to fallback",
                    operatorName, e);
            return false;
        }
    }

    // BucketAssigner 持有的 RowDataPartitionComputer 上与 OperatorDescriptorHelper
    // 同源的 partitionIndexes。
    private static int[] partitionIndexesOf(StreamOperator<?> operator) {
        Object bucketsBuilder = ReflectionUtils.retrievePrivateField(operator, "bucketsBuilder");
        Object assigner = ReflectionUtils.retrievePrivateField(bucketsBuilder, "bucketAssigner");
        Object computer = ReflectionUtils.retrievePrivateField(assigner, "computer");
        return (int[]) ReflectionUtils.retrievePrivateField(computer, "partitionIndexes");
    }

    // 与 VectorBatchUtil::getValueAtAsStr 中产出 vanilla 等价文本的分支一致：
    // VARCHAR/CHAR、INT/LONG、BOOLEAN；TIMESTAMP、DECIMAL、DOUBLE 不在此列。
    private static boolean isNativePartitionValueRenderable(LogicalType type) {
        return type instanceof VarCharType || type instanceof CharType
                || type instanceof IntType || type instanceof BigIntType || type instanceof BooleanType;
    }

    // 取节点行输入（sink writer 收到的列）的逻辑类型；无 row serializer 时返回 null。
    private static LogicalType[] collectInputLogicalTypes(StreamNode node) {
        TypeSerializer<?>[] serializers = node.getTypeSerializersIn();
        if (serializers == null) {
            return null;
        }
        for (TypeSerializer<?> serializer : serializers) {
            if (serializer instanceof AbstractRowDataSerializer) {
                try {
                    return ReflectionUtils.retrievePrivateField(serializer, "types");
                } catch (RuntimeException e) {
                    LOG.warn("Cannot read row types of CSV file sink input", e);
                    return null;
                }
            }
        }
        return null;
    }

    // 单字节 ASCII、非空、非 NUL：native 的 char 表示能无损承载的集合。
    private static boolean isSingleAsciiChar(String value) {
        return value != null && value.length() == 1
                && value.charAt(0) != '\0' && value.charAt(0) <= 0x7F;
    }

    private static boolean validateSink(String operatorName, StreamOperatorFactory operatorFactory) {
        if (!isSinkSupportNative) {
            return false;
        }
        if (jobHasCsvFileSink && requiresFlinkFileSink(operatorName, operatorFactory)
                && !isFileSinkCsvNative) {
            return false;
        }
        if (!(operatorFactory instanceof SinkWriterOperatorFactory)) {
            return true;
        }
        Sink sink = ((SinkWriterOperatorFactory) operatorFactory).getSink();
        if (!sink.getClass().getSimpleName().equals("KafkaSink")) {
            return true;
        }
        Object recordSerializer = ReflectionUtils.retrievePrivateField(sink, "recordSerializer");
        Object valueSerializationSchema = ReflectionUtils
                .retrievePrivateField(recordSerializer, "valueSerialization");
        return "JsonRowDataSerializationSchema".equals(valueSerializationSchema.getClass().getSimpleName());
    }

    private static boolean validateSource(String operatorDescription, StreamOperatorFactory operatorFactory) {
        if (!isSourceSupportNative) {
            return false;
        }
        if (operatorFactory instanceof SourceOperatorFactory) {
            Source source = ReflectionUtils.retrievePrivateField(operatorFactory, "source");
            String sourceName = source.getClass().getSimpleName();
            if ("FileSource".equals(sourceName)) {
                Object readerFormat = ReflectionUtils.retrievePrivateField(source, "readerFormat");
                if (!(readerFormat instanceof org.apache.flink.connector.file.src.impl.StreamFormatAdapter)) {
                    LOG.info("FileSource reader format {} has no native descriptor implementation; using Flink",
                            readerFormat == null ? "null" : readerFormat.getClass().getSimpleName());
                    return false;
                }
            }
            if (!"KafkaSource".equals(sourceName) && !"NexmarkSource".equals(sourceName)
                    && !"FileSource".equals(sourceName)) {
                LOG.info("Source {} has no native descriptor implementation; using Flink", sourceName);
                return false;
            }
        }
        // VALUES-based sources (SimpleInputFormatOperatorFactory) are not supported
        // in native SQL execution because OmniStream does not handle them
        if (operatorFactory instanceof SimpleInputFormatOperatorFactory) {
            LOG.info("validateSource: VALUES-based source (SimpleInputFormatOperatorFactory) is not supported for native execution");
            return false;
        }
        if (!operatorDescription.contains("originDescription")) {
            return true;
        }
        Map<String, Object> jsonMap = toJsonMap(operatorDescription);
        /*
         * Check if jsonMap is null to prevent NullPointerException
         */
        if (jsonMap == null) {
            return false;
        }
        if (!jsonMap.containsKey("deserializationSchema")
                || !(jsonMap.get("deserializationSchema") instanceof String)
                || !SUPPORT_KAFKA_SCHEMA_TYPE.contains((String) jsonMap.get("deserializationSchema"))) {
            return false;
        }
        if (!jsonMap.containsKey("hasMetadata") || !(jsonMap.get("hasMetadata") instanceof Boolean)
                || (Boolean) jsonMap.get("hasMetadata")) {
            return false;
        }
        return jsonMap.containsKey("watermarkStrategy") && (jsonMap.get("watermarkStrategy") instanceof String)
                && !((String) jsonMap.get("watermarkStrategy")).isEmpty();
    }

    private static Map<String, Object> toJsonMap(String operatorDescription) {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        Map<String, Object> jsonMap = null;
        try {
            jsonMap = objectMapper.readValue(operatorDescription, new TypeReference<Map<String, Object>>() {
            });
        } catch (JsonProcessingException e) {
            LOG.warn("operatorDescription read jsonString to Map failed!", e);
        }
        return jsonMap;
    }

    /** Find terminal sink stages without absorbing ordinary computation into the sink. */
    static Set<Integer> collectSinkFallbackNodes(
            Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos) {
        Map<Integer, StreamNode> nodes = new HashMap<>();
        for (StreamingJobGraphGenerator.OperatorChainInfo chain : chainInfos.values()) {
            if (chain != null) {
                for (StreamNode node : chain.getAllChainedNodes()) {
                    nodes.put(node.getId(), node);
                }
            }
        }
        Set<Integer> terminalStages = new HashSet<>();
        boolean changed;
        do {
            changed = false;
            for (StreamNode node : nodes.values()) {
                if (!terminalStages.contains(node.getId()) && isSinkStage(node)
                        && node.getOutEdges().stream().allMatch(
                                edge -> terminalStages.contains(edge.getTargetId()))) {
                    terminalStages.add(node.getId());
                    changed = true;
                }
            }
        } while (changed);

        // Writers/committers and Paimon adapters use Flink, including downstream end stages.
        Set<Integer> fallback = new HashSet<>();
        Queue<Integer> pending = new LinkedList<>();
        for (Integer id : terminalStages) {
            StreamNode node = nodes.get(id);
            if (isWriterOrCommitter(node) || isPaimonSinkAdapter(node)) {
                pending.add(id);
            }
        }
        while (!pending.isEmpty()) {
            Integer id = pending.poll();
            if (!fallback.add(id)) {
                continue;
            }
            for (StreamEdge edge : nodes.get(id).getOutEdges()) {
                pending.add(edge.getTargetId());
            }
        }
        return fallback;
    }

    private static boolean isSinkStage(StreamNode node) {
        String name = node.getOperatorName();
        return (name != null && isSink(name)) || isWriterOrCommitter(node) || isPaimonSinkAdapter(node);
    }

    private static boolean isWriterOrCommitter(StreamNode node) {
        // Operator names alone are not enough: native-capable sinks such as JDQ
        // may also be named "Writer" or "Committer". Only force known Flink
        // file-sink stages or Paimon stages to the stock runtime here.
        String name = node.getOperatorName();
        return requiresFlinkFileSink(name, node.getOperatorFactory())
                || (name != null && name.toLowerCase(Locale.ROOT).contains("paimon"));
    }

    private static boolean isPaimonSinkAdapter(StreamNode node) {
        if (!(node.getOperatorFactory() instanceof SimpleOperatorFactory)) {
            return false;
        }
        StreamOperator<?> operator = node.getOperator();
        if (!(operator instanceof StreamMap)) {
            return false;
        }
        Object function = ((StreamMap<?, ?>) operator).getUserFunction();
        // No Paimon dependency: recognize only its builder-generated conversion lambdas.
        return function != null && function.getClass().getName().startsWith(
                "org.apache.paimon.flink.sink.FlinkSinkBuilder$$Lambda$");
    }

    static boolean isFallbackEndpoint(StreamNode node) {
        String name = node.getOperatorName();
        if (name == null) {
            return false;
        }
        // A source/sink name alone must not turn an internal operator into a boundary.
        return (isSource(name) && node.getInEdges().isEmpty())
                || (isSink(name) && node.getOutEdges().isEmpty());
    }

    public static boolean isSource(String operatorName) {
        Matcher matcher = SOURCE_REGEX.matcher(operatorName);
        return matcher.find();
    }

    public static boolean isSink(String operatorName) {
        Matcher matcher = SINK_REGEX.matcher(operatorName);
        return matcher.find();
    }

    private static boolean isConstraintEnforcer(String operatorName) {
        return operatorName.contains("ConstraintEnforcer");
    }

    private static boolean isStreamRecordTimestampInserter(String operatorName) {
        return operatorName.contains("StreamRecordTimestampInserter");
    }

    private static boolean isPartitionCommitter(String operatorName) {
        return operatorName.equals("PartitionCommitter");
    }

    private static String extractOperatorName(String operatorName) {
        Matcher matcher = operatorName.contains("WatermarkAssigner") || operatorName.contains("GroupAggregate")
                ? WATERMARK_OP_NAME_REGEX.matcher(operatorName) : OPERATOR_NAME_REGEX.matcher(operatorName);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return "UNKNOWN_OPERATOR_IMPOSSIBLE_MATCH";
        }
    }

    /**
     * 检查给定的算子链信息中是否包含特定类型的算子。
     *
     * @param chainInfos            算子链信息的映射
     * @param operatorTypePredicate 一个 Predicate，用于判断 StreamOperator 是否是目标类型
     * @return 如果找到至少一个匹配的算子，则返回 true，否则返回 false
     */
    private static boolean containsConversionOperator(
            Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos,
            Predicate<StreamOperator<?>> operatorTypePredicate) {

        for (StreamingJobGraphGenerator.OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamNode> chainedNodes = chainInfo.getAllChainedNodes();
            if (containsConversionOperator(chainedNodes, operatorTypePredicate)) {
                return true;
            }
        }
        return false; // 未找到
    }

    // InputConversionOperator： 将 DataStream 的 Java 对象转换为 Table API/SQL 的 RowData 格式。
    private static boolean containsDataStreamToTable(
            Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos) {
        return containsConversionOperator(chainInfos, operator -> operator instanceof InputConversionOperator);
    }

    // OutputConversionOperator： 将 Table API/SQL 的 RowData 格式转换为 DataStream 的 Java 对象。
    private static boolean containsTableToDataStream(
            Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos) {
        return containsConversionOperator(chainInfos, operator -> operator instanceof OutputConversionOperator);
    }

    private static boolean containsConversionOperator(List<StreamNode> chainedNode,
                                                      Predicate<StreamOperator<?>> operatorTypePredicate) {
        // the operator is in reverse order within chainedNode
        for (int i = chainedNode.size() - 1; i >= 0; i--) {
            StreamOperator<?> operator = chainedNode.get(i).getOperator();
            if (operatorTypePredicate.test(operator)) {
                return true; // 找到即返回
            }
        }
        return false; // 未找到
    }

    // InputConversionOperator： 将 DataStream 的 Java 对象 转换为 Table API/SQL 的 RowData 格式。
    private static boolean containsDataStreamToTable(List<StreamNode> chainedNode) {
        return containsConversionOperator(chainedNode, operator -> operator instanceof InputConversionOperator);
    }

    // OutputConversionOperator： 将 Table API/SQL 的 RowData 格式 转换为 DataStream 的 Java 对象。
    private static boolean containsTableToDataStream(List<StreamNode> chainedNode) {
        return containsConversionOperator(chainedNode, operator -> operator instanceof OutputConversionOperator);
    }

    private static boolean containsTableStreamOperator(Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos) {
        Set<Integer> sinkFallbackNodes = collectSinkFallbackNodes(chainInfos);
        boolean onlySourceAndSink = true;
        List<String> inputTypeList = new ArrayList<>();
        for (StreamingJobGraphGenerator.OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamNode> chainedNode = chainInfo.getAllChainedNodes();
            for (int i = chainedNode.size() - 1; i >= 0; i--) {

                StreamNode node = chainedNode.get(i);
                if (sinkFallbackNodes.contains(node.getId())) {
                    continue;
                }
                String operatorName = node.getOperatorName();
                if (isSource(operatorName)) {
                    if (isSourceSql(node, operatorName)) {
                        getSourceJobType(node, operatorName, jobType, inputTypeList);
                        continue;
                    } else {
                        return false;
                    }
                }
                if (isSink(operatorName)) {
                    if (isSinkSql(node, operatorName)) {
                        getInputTypes(node);
                        continue;
                    } else {
                        // A terminal sink whose input is not RowData (e.g. "Sink: end" backed by a
                        // PojoSerializer) is a pass-through and must not degrade the whole job to
                        // STREAM; SQL-mode validateSink already accepts such non-Kafka sinks.
                        continue;
                    }
                }
                if (isConstraintEnforcer(operatorName) || isStreamRecordTimestampInserter(operatorName)) {
                    getInputTypes(node);
                    continue;
                }
                if (operatorName.contains(": Committer")) {
                    continue;
                }
                onlySourceAndSink = false;
                String opSimpleName = extractOperatorName(operatorName);
                if (!OP_NAME_OF_SQL.contains(opSimpleName)) {
                    return false;
                }
            }
        }
        if (onlySourceAndSink) {
            ValidateCalcOPStrategy validate = new ValidateCalcOPStrategy();
            if (!inputTypeList.isEmpty() && !validate.validateDataTypes(Collections.singletonList(inputTypeList))) {
                jobType = jobType.getCombinationsJobType(JobType.SQL_STREAM);
            }
        }
        return true;
    }

    public enum Tag {
        NORMAL,
        OUTPUT_CONVERSION_OPERATOR,
        INPUT_CONVERSION_OPERATOR
    }

    // BFS 状态，用于记录节点、标记、距离和方向
    static class BFSState {
        StreamNode node;
        OperatorType operatorType;
        int distance; // 距离起始转换算子的跳数
        boolean isForward; // true: 向前传播 (子节点), false: 向后传播 (父节点)
        Tag originConverterType; // 标记的来源转换算子类型

        public BFSState(StreamNode node, OperatorType operatorType, int distance, boolean isForward, Tag originConverterType) {
            this.node = node;
            this.operatorType = operatorType;
            this.distance = distance;
            this.isForward = isForward;
            this.originConverterType = originConverterType;
        }
    }

    /**
     * 根据拓扑结构和转换算子类型对所有算子进行标记。
     * 算法思想：多源 BFS，记录距离，处理冲突时优先采纳“距离最近”的规则。
     * 如果距离相同，则根据测试用例的期望，确定特定转换算子类型的优先级。
     *
     * @param chainedNode 所有算子列表，包含拓扑关系。
     * @return 一个 Map，键为算子ID，值为其标记。
     */
    private static Map<Integer, OperatorType> tagOperators(List<StreamNode> chainedNode, Map<Integer, StreamNode> operatorMap) {
        Map<Integer, OperatorType> finalOperatorTags = new HashMap<>();
        // 记录每个算子被标记的最小距离，用于解决冲突
        Map<Integer, Integer> minDistance = new HashMap<>();
        // 记录每个算子被哪个转换算子类型（Output/Input）标记，用于冲突解决
        Map<Integer, Tag> tagSourceConverterType = new HashMap<>();

        // 初始化所有算子为 NULL, 且距离无限大
        for (int i = chainedNode.size() - 1; i >= 0; i--) {
            finalOperatorTags.put(chainedNode.get(i).getId(), OperatorType.NULL);
            minDistance.put(chainedNode.get(i).getId(), Integer.MAX_VALUE);
        }

        // 构建逆拓扑图，方便向前遍历
        Map<StreamNode, List<StreamNode>> reverseGraph = buildReverseGraph(chainedNode, operatorMap);

        // BFS 队列
        Queue<BFSState> queue = new LinkedList<>();

        // 1. 初始化队列，将所有转换算子加入
        for (int i = chainedNode.size() - 1; i >= 0; i--) {
            StreamNode node = chainedNode.get(i);
            if (node.getOperator() instanceof OutputConversionOperator) {
                // OutputConversionOperator 自身标记为 STREAM
                finalOperatorTags.put(node.getId(), OperatorType.STREAM);
                minDistance.put(node.getId(), 0); // 距离自身为0
                tagSourceConverterType.put(node.getId(), Tag.OUTPUT_CONVERSION_OPERATOR);

                // 传播 STREAM 向后
                queue.offer(new BFSState(node, OperatorType.STREAM, 0, true, Tag.OUTPUT_CONVERSION_OPERATOR));
                // 传播 SQL 向前
                queue.offer(new BFSState(node, OperatorType.SQL, 0, false, Tag.OUTPUT_CONVERSION_OPERATOR));

            } else if (node.getOperator() instanceof InputConversionOperator) {
                // InputConversionOperator 自身标记为 SQL
                finalOperatorTags.put(node.getId(), OperatorType.SQL);
                minDistance.put(node.getId(), 0);
                tagSourceConverterType.put(node.getId(), Tag.INPUT_CONVERSION_OPERATOR);

                // 传播 SQL 向后
                queue.offer(new BFSState(node, OperatorType.SQL, 0, true, Tag.INPUT_CONVERSION_OPERATOR));
                // 传播 STREAM 向前
                queue.offer(new BFSState(node, OperatorType.STREAM, 0, false, Tag.INPUT_CONVERSION_OPERATOR));
            }
        }

        // 2. 执行 BFS 传播
        while (!queue.isEmpty()) {
            BFSState current = queue.poll();
            StreamNode currentNode = current.node;
            OperatorType newOperatorType = current.operatorType;
            int newDistance = current.distance + 1;
            Tag newSourceType = current.originConverterType;

            // 确定要遍历的相邻节点列表
            List<StreamNode> nextOps = new ArrayList<>();
            if (current.isForward) {
                for (StreamEdge edge : currentNode.getOutEdges()) {
                    if (existInOperatorChain(edge.getTargetId(), operatorMap)) {
                        StreamNode child = operatorMap.get(edge.getTargetId());
                        nextOps.add(child);
                    }
                }
            } else {
                nextOps.addAll(reverseGraph.getOrDefault(currentNode, Collections.emptyList()));
            }

            for (StreamNode nextOp : nextOps) {
                // 核心冲突解决逻辑：
                // a. 如果目标节点尚未被标记（NULL），直接标记
                // b. 如果目标节点已经被标记，但新路径距离更短，则覆盖
                // c. 如果距离相同，则根据源转换算子类型和目标标记类型决定是否覆盖

                OperatorType existingOperatorType = finalOperatorTags.get(nextOp.getId());
                Integer existingDistance = minDistance.get(nextOp.getId());

                boolean shouldUpdate = false;

                if (existingOperatorType == OperatorType.NULL) {
                    shouldUpdate = true; // 尚未标记，直接标记
                } else if (newDistance < existingDistance) {
                    shouldUpdate = true; // 距离更短，覆盖; 防止转换算子的类型被更新
                } else if (newDistance == existingDistance) {
                    // 距离相同，处理冲突。原生的执行计划保证了这里不会遇到冲突
                    if (newOperatorType == OperatorType.STREAM && existingOperatorType == OperatorType.SQL && newSourceType == Tag.OUTPUT_CONVERSION_OPERATOR) {
                        shouldUpdate = false;
                    } else if (newOperatorType == OperatorType.SQL && existingOperatorType == OperatorType.STREAM && newSourceType == Tag.INPUT_CONVERSION_OPERATOR) {
                        shouldUpdate = false;
                    } else {
                        // existingOperatorType == OperatorType.SQL && newSourceType == Tag.INPUT_CONVERSION_OPERATOR situation don't exist
                        // existingOperatorType == OperatorType.STREAM && newSourceType == Tag.OUTPUT_CONVERSION_OPERATOR situation don't exist
                        shouldUpdate = false;
                    }
                }

                if (shouldUpdate) {
                    finalOperatorTags.put(nextOp.getId(), newOperatorType);
                    minDistance.put(nextOp.getId(), newDistance);
                    tagSourceConverterType.put(nextOp.getId(), newSourceType);
                    queue.offer(new BFSState(nextOp, newOperatorType, newDistance, current.isForward, newSourceType));
                }
            }
        }

        return finalOperatorTags;
    }


    /**
     * 构建逆拓扑图，方便向前（父节点方向）遍历。
     *
     * @param chainedNode 所有算子列表。
     * @return 逆拓扑图，键为子节点，值为其父节点列表。
     */
    private static Map<StreamNode, List<StreamNode>> buildReverseGraph(List<StreamNode> chainedNode, Map<Integer, StreamNode> operatorMap) {
        Map<StreamNode, List<StreamNode>> reverseGraph = new HashMap<>();
        // 初始化所有算子都有一个空的父节点列表
        for (int i = chainedNode.size() - 1; i >= 0; i--) {
            StreamNode node = chainedNode.get(i);
            reverseGraph.put(node, new ArrayList<>());
        }

        // 遍历所有算子，填充逆拓扑图
        for (int i = chainedNode.size() - 1; i >= 0; i--) {
            StreamNode node = chainedNode.get(i);
            for (StreamEdge edge : node.getOutEdges()) {
                if (existInOperatorChain(edge.getTargetId(), operatorMap)) {
                    StreamNode child = operatorMap.get(edge.getTargetId());
                    reverseGraph.get(child).add(node);
                }
            }
        }
        return reverseGraph;
    }

    private static void setOperatorType(Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos) {
        OperatorType currentType = getInitOperatorType(chainInfos);
        for (StreamingJobGraphGenerator.OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamNode> chainedNode = chainInfo.getAllChainedNodes();
            TaskType taskType = TaskType.NULL;
            Map<Integer, StreamNode> operatorMap = getOperatorMap(chainedNode);

            // chainedNode is a directed acyclic graph(dag)
            boolean containsStreamToTable = containsDataStreamToTable(chainedNode);
            boolean containsTableToStream = containsTableToDataStream(chainedNode);
            if (containsStreamToTable && containsTableToStream) {
                // tagOperators: tag each operator type
                Map<Integer, OperatorType> operatorTypeMap = tagOperators(chainedNode, operatorMap);
                for (Map.Entry<Integer, OperatorType> entry : operatorTypeMap.entrySet()) {
                    taskType = taskType.getCombinationsOperatorType(entry.getValue());
                }
                operatorTypes.add(operatorTypeMap);
                // find which conversion operator comes last.
                currentType = lastOperatorType(chainedNode);
            } else if (containsStreamToTable) {
                // tagOperators: tag each operator type
                Map<Integer, OperatorType> operatorTypeMap = tagOperators(chainedNode, operatorMap);
                for (Map.Entry<Integer, OperatorType> entry : operatorTypeMap.entrySet()) {
                    taskType = taskType.getCombinationsOperatorType(entry.getValue());
                }
                operatorTypes.add(operatorTypeMap);
                currentType = OperatorType.SQL;
            } else if (containsTableToStream) {
                // tagOperators: tag each operator type
                Map<Integer, OperatorType> operatorTypeMap = tagOperators(chainedNode, operatorMap);
                for (Map.Entry<Integer, OperatorType> entry : operatorTypeMap.entrySet()) {
                    taskType = taskType.getCombinationsOperatorType(entry.getValue());
                }
                operatorTypes.add(operatorTypeMap);
                currentType = OperatorType.STREAM;
            } else {
                Map<Integer, OperatorType> operatorTypeMap = new HashMap<>();
                for (StreamNode node : chainedNode) {
                    operatorTypeMap.put(node.getId(), currentType);
                }
                taskType = taskType.getCombinationsOperatorType(currentType);
                operatorTypes.add(operatorTypeMap);
            }

            taskTypes.add(taskType);
            jobType = jobType.getCombinationsTaskType(taskType);
        }
    }

    private static Map<Integer, StreamNode> getOperatorMap(List<StreamNode> chainedNode) {
        Map<Integer, StreamNode> operatorMap = new HashMap<>();
        for (StreamNode node : chainedNode) {
            operatorMap.put(node.getId(), node);
        }
        return operatorMap;
    }

    private static OperatorType getInitOperatorType(Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos) {
        for (StreamingJobGraphGenerator.OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamNode> chainedNode = chainInfo.getAllChainedNodes();
            boolean containsStreamToTable = containsDataStreamToTable(chainedNode);
            boolean containsTableToStream = containsTableToDataStream(chainedNode);
            if (containsStreamToTable && containsTableToStream) {
                // find which conversion operator comes first.
                return firstOperatorType(chainedNode);
            } else if (containsStreamToTable) {
                return OperatorType.STREAM;
            } else if (containsTableToStream) {
                return OperatorType.SQL;
            }
        }
        // It is impossible to happen.
        throw new FlinkRuntimeException("getInitOperatorType api: It is impossible to happen");
    }

    // find which conversion operator comes first.
    private static OperatorType firstOperatorType(List<StreamNode> chainedNode) {
        // the operator is in reverse order within chainedNode
        for (int i = chainedNode.size() - 1; i >= 0; i--) {
            if (chainedNode.get(i).getOperator() instanceof OutputConversionOperator) {
                return OperatorType.STREAM;
            } else if (chainedNode.get(i).getOperator() instanceof InputConversionOperator) {
                return OperatorType.SQL;
            }
        }
        // It is impossible to happen.
        throw new FlinkRuntimeException("firstOperatorType api: It is impossible to happen");
    }

    // find which conversion operator comes last.
    private static OperatorType lastOperatorType(List<StreamNode> chainedNode) {
        // the operator is in reverse order within chainedNode
        for (int i = 0; i < chainedNode.size(); i++) {
            if (chainedNode.get(i).getOperator() instanceof OutputConversionOperator) {
                return OperatorType.SQL;
            } else if (chainedNode.get(i).getOperator() instanceof InputConversionOperator) {
                return OperatorType.STREAM;
            }
        }
        // It is impossible to happen.
        throw new FlinkRuntimeException("lastOperatorType api: It is impossible to happen");
    }

    private static boolean existInOperatorChain(Integer targetId, Map<Integer, StreamNode> operatorMap) {
        return operatorMap.containsKey(targetId);
    }

    private static void setAllOperatorType(Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos, OperatorType operatorType) {
        for (StreamingJobGraphGenerator.OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamNode> chainedNode = chainInfo.getAllChainedNodes();

            TaskType taskType = TaskType.NULL;
            Map<Integer, OperatorType> operatorTypeMap = new HashMap<>();

            for (StreamNode node : chainedNode) {
                operatorTypeMap.put(node.getId(), operatorType);
            }
            operatorTypes.add(operatorTypeMap);
            taskType = taskType.getCombinationsOperatorType(operatorType);
            taskTypes.add(taskType);
            jobType = jobType.getCombinationsTaskType(taskType);
        }
    }

    public static void setdataStreamLowParaMode(boolean isBatchMode) {
        DATASTREAM_LOW_PARA_MODE = isBatchMode;
    }
}
