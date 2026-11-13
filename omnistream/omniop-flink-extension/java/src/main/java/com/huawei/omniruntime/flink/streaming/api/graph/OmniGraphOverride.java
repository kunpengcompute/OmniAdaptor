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

import com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy.AbstractValidateOperatorStrategy;
import com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy.ValidateCalcOPStrategy;
import com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy.ValidateChangelogNormalizeOPStrategy;
import com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy.ValidateOperatorStrategyFactory;
import com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy.ValidateValuesOPStrategy;
import com.huawei.omniruntime.flink.utils.DescriptionUtil;
import com.huawei.omniruntime.flink.utils.ReflectionUtils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
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
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.table.runtime.operators.sink.OutputConversionOperator;
import org.apache.flink.table.runtime.operators.source.InputConversionOperator;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
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
    private static final Set<String> SUPPORT_OP_NAME_BASELINE = new HashSet<>();
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
                    "LookupJoin",
                    "WatermarkAssigner",
                    "StreamRecordTimestampInserter",
                    "ConstraintEnforcer",
                    "GroupAggregate"));
        } else {
            SUPPORT_OP_NAME.addAll(Arrays.asList(
                    "Calc",
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
                    "Limit",
                    "WindowTableFunction",
                    "StreamRecordTimestampInserter",
                    "ConstraintEnforcer",
                    "ChangelogNormalize"));
        }
        SUPPORT_KAFKA_SCHEMA_TYPE.add("JsonRowDataDeserializationSchema");
        OP_NAME_OF_SQL.addAll(Arrays.asList("Calc", "GroupAggregate", "LocalGroupAggregate", "GlobalGroupAggregate",
                "IncrementalGroupAggregate", "Join", "LookupJoin", "WindowAggregate", "WindowJoin", "GroupWindowAggregate",
                "Deduplicate", "Expand", "GlobalWindowAggregate", "LocalWindowAggregate", "WatermarkAssigner", "Rank",
                "Limit", "WindowTableFunction", "StreamRecordTimestampInserter", "ConstraintEnforcer", "ChangelogNormalize"));
        // Snapshot the baseline whitelist determined by performanceMode, so that setStateBackend()/setNext()
        // restrictions can be reset per job. Must be taken inside the static block AFTER SUPPORT_OP_NAME is
        // populated (field initializers would capture the still-empty set).
        SUPPORT_OP_NAME_BASELINE.addAll(SUPPORT_OP_NAME);
    }

    private static boolean isSourceSupportNative = true;

    private static boolean isSinkSupportNative = true;

    private static boolean isConstraintEnforcerSupportNative = true;

    private static boolean isStreamRecordTimestampInserterSupportNative = true;

    // native 时间类型是 int64 毫秒，因此只能表示精度 <= 3。
    // TIME(0..3) 现已加入（时间向量化已落地）；TIMESTAMP(9)（纳秒）被移除，因为 native
    // 无法承载——声明支持它会导致静默回退或数据丢失。下方 SINK_SUPPORT_DATA_TYPE 遵循相同的精度规则。
    private static final Set<String> SOURCE_SUPPORT_DATA_TYPE = new HashSet<>(Arrays.asList(
            "BIGINT",
            "INTEGER",
            "DOUBLE",
            "DATE",
            "BOOLEAN",
            "TIME_WITHOUT_TIME_ZONE(0)",
            "TIME_WITHOUT_TIME_ZONE(1)",
            "TIME_WITHOUT_TIME_ZONE(2)",
            "TIME_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(0)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(1)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(2)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(9)",
            "VARCHAR(2147483647)",
            "VARCHAR(2000)",
            "VARCHAR(9)",
            "VARCHAR",
            "CHAR",
            "STRING",
            "DECIMAL64",
            "DECIMAL128",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(1)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(2)",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)"));

    // 与上方 SOURCE_SUPPORT_DATA_TYPE 相同的时间精度规则（仅精度 <= 3）。
    // 不支持的 sink 类型会静默地把 isSinkSupportNative 置为 false，使整条链回退到 Flink，
    // 因此精度上限必须与 native 的 int64-毫秒表示保持同步。
    private static final Set<String> SINK_SUPPORT_DATA_TYPE = new HashSet<>(Arrays.asList(
            "BIGINT",
            "INTEGER",
            "DOUBLE",
            "DATE",
            "BOOLEAN",
            "TIME_WITHOUT_TIME_ZONE(0)",
            "TIME_WITHOUT_TIME_ZONE(1)",
            "TIME_WITHOUT_TIME_ZONE(2)",
            "TIME_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(0)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(1)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(2)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(9)",
            "VARCHAR(2147483647)",
            "VARCHAR(2000)",
            "VARCHAR(9)",
            "VARCHAR",
            "CHAR",
            "STRING",
            "MAP",
            "DECIMAL64",
            "DECIMAL128",
            "BINARY",
            "VARBINARY",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)"));

    private static boolean isIntervalOutputType(String type) {
        return type != null && type.matches("^INTERVAL(?: |_).*$");
    }

    /**
     * setStateBackend
     *
     * @param stateBackend backend
     */
    public static void setStateBackend(StateBackend stateBackend) {
        if (performanceMode && stateBackend != null && !(stateBackend instanceof HashMapStateBackend)) {
            SUPPORT_OP_NAME.clear();
            SUPPORT_OP_NAME.addAll(
                    Arrays.asList("Calc", "WatermarkAssigner", "StreamRecordTimestampInserter",
                            "ConstraintEnforcer"));
        }
    }


    public static void setNext() {
        if (performanceMode) {
            SUPPORT_OP_NAME.clear();
            SUPPORT_OP_NAME.addAll(
                    Arrays.asList("Calc", "WatermarkAssigner", "StreamRecordTimestampInserter",
                            "ConstraintEnforcer"));
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

        StreamConfig vertexConfig = new StreamConfig(vertexEntry.getValue().getConfiguration());
        Integer vertexID = vertexEntry.getKey();
        LOG.info("validateVertexForOmniTask : vertexID is {}, and vertexName {}",
                vertexID, vertexConfig.getOperatorName());

        JobVertex jobVertex = vertexEntry.getValue();

        if (validateVertexChainInfoForOmniTask(vertexID, chainInfos, chainedConfigs, jobVertex, vertexConfigs, jobType)) {
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
                String operatorName = node.getOperatorName();
                boolean isWindow = operatorName != null && operatorName.contains("Window");
                nowChainDoSplit = nowChainDoSplit || isWindow || containsCurrentWatermark(node);
            }
            vertexConfig.setSplitWatermark(otherChainDoSplit || nowChainDoSplit);
        }
        return vertexConfig.isSplitWatermark();
    }

    private static boolean containsCurrentWatermark(StreamNode node) {
        String operatorDescription = node.getOperatorDescription();
        if (operatorDescription == null || !operatorDescription.trim().startsWith("{")) {
            return false;
        }
        Map<String, Object> descriptionMap = toJsonMap(operatorDescription);
        return descriptionMap != null && containsCurrentWatermark(descriptionMap);
    }

    private static boolean containsCurrentWatermark(Object value) {
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            Object functionName = map.get("function_name");
            if (functionName instanceof String
                    && "current_watermark".equalsIgnoreCase((String) functionName)) {
                return true;
            }
            for (Object child : map.values()) {
                if (containsCurrentWatermark(child)) {
                    return true;
                }
            }
            return false;
        }
        if (value instanceof Iterable) {
            for (Object child : (Iterable<?>) value) {
                if (containsCurrentWatermark(child)) {
                    return true;
                }
            }
            return false;
        }
        return false;
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
                                                              Map<Integer, StreamConfig> vertexConfigs, JobType jobType) {
        // walkthrough each operator
        StreamingJobGraphGenerator.OperatorChainInfo chainInfo = chainInfos.get(vertexID);
        if (chainInfo == null) {
            LOG.info("validateVertexChainInfoForOmniTask chainInfo of vertex ID {} is null", vertexID);
            return false;
        }

        StreamGraph streamGraph = chainInfo.getStreamGraph();
        List<StreamNode> chainedNode = chainInfo.getAllChainedNodes();
        for (StreamNode node : chainedNode) {
            if (validateNodeForOmniTask(vertexID, vertexConfigs, jobType, streamGraph, node)) {
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

    private static boolean validateNodeForOmniTask(Integer vertexID, Map<Integer, StreamConfig> vertexConfigs,
                                                   JobType jobType, StreamGraph streamGraph, StreamNode node) {
        String operatorName = node.getOperatorName();
        String operatorDescription = node.getOperatorDescription();
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
        jobType = JobType.NULL;
        taskTypes.clear();
        operatorTypes.clear();
        supportTaskFallback = true;
        isSourceSupportNative = true;
        isSinkSupportNative = true;
        isConstraintEnforcerSupportNative = true;
        isStreamRecordTimestampInserterSupportNative = true;
        resetSupportedOpNames();
    }

    private static void resetSupportedOpNames() {
        SUPPORT_OP_NAME.clear();
        SUPPORT_OP_NAME.addAll(SUPPORT_OP_NAME_BASELINE);
    }

    public static JobType getJobType(Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos) {
        JobType jobType = JobType.NULL;
        boolean onlySourceAndSink = true;
        List<String> inputTypeList = new ArrayList<>();
        for (StreamingJobGraphGenerator.OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamNode> chainedNode = chainInfo.getAllChainedNodes();
            for (StreamNode node : chainedNode) {
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
            if (type.matches("^DECIMAL64\\([^)]*\\)$")) {
                type = "DECIMAL64";
                LOG.info("converted to DECIMAL64");
            }
            if (type.matches("^DECIMAL128\\([^)]*\\)$")) {
                type = "DECIMAL128";
                LOG.info("converted to DECIMAL128");
            }
            if (type.matches("^CHAR\\([^)]*\\)$")) {
                type = "CHAR";
                LOG.info("converted to CHAR");
            }
            if (!SOURCE_SUPPORT_DATA_TYPE.contains(type)) {
                if (type.matches("^VARCHAR\\([^)]*\\)$")) {
                    type = "VARCHAR";
                    LOG.info("converted to VARCHAR");
                }
            }
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
                LOG.info("getInputTypes normalized DECIMAL64: '{}' -> 'DECIMAL64'", type);
                type = "DECIMAL64";
            }

            if (type.matches("^DECIMAL128\\([^)]*\\)$")) {
                LOG.info("getInputTypes normalized DECIMAL128: '{}' -> 'DECIMAL128'", type);
                type = "DECIMAL128";
            }
            if (type.matches("^CHAR\\([^)]*\\)$")) {
                type = "CHAR";
                LOG.info("converted to CHAR");
            }
            if (!SINK_SUPPORT_DATA_TYPE.contains(type)) {
                if (type.matches("^VARCHAR\\([^)]*\\)$")) {
                    type = "VARCHAR";
                    LOG.info("converted to VARCHAR");
                }
            }
            isSupportNative = SINK_SUPPORT_DATA_TYPE.contains(type) || isIntervalOutputType(type);
            if (!isSupportNative) {
                LOG.info("getInputTypes unsupported sink type for operator '{}': '{}' (original: '{}')", operatorName, type, originalType);
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
            return validateSink(operatorName, operatorDescription, operatorFactory);
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
            if (jsonMap == null) {
                LOG.info("The operator description is not valid JSON: {}", operatorDescription);
                return false;
            }

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

    public static boolean isUpsertKafkaDescription(String description) {
        if (description == null || !description.contains("\"upsertMode\"")) {
            return false;
        }
        Map<String, Object> plan = toJsonMap(description);
        return plan != null && Boolean.TRUE.equals(plan.get("upsertMode"));
    }

    private static boolean validateSink(String operatorName, String description, StreamOperatorFactory operatorFactory) {
        if (!isSinkSupportNative) {
            return false;
        }
        if (performanceMode && operatorName.contains("StreamingFileWriter")) {
            return false;
        }
        boolean upsert = isUpsertKafkaDescription(description);
        if (!(operatorFactory instanceof SinkWriterOperatorFactory)) {
            return true;
        }
        Sink sink = ((SinkWriterOperatorFactory) operatorFactory).getSink();
        if (!sink.getClass().getSimpleName().equals("KafkaSink")) {
            return !upsert;
        }
        Object recordSerializer = ReflectionUtils.retrievePrivateField(sink, "recordSerializer");
        Object valueSerialization = ReflectionUtils
                .retrievePrivateField(recordSerializer, "valueSerialization");
        if (valueSerialization == null
                || !"JsonRowDataSerializationSchema".equals(valueSerialization.getClass().getSimpleName())) {
            return false;
        }
        if (!upsert) {
            return true;
        }
        Object keySerialization = ReflectionUtils.retrievePrivateField(recordSerializer, "keySerialization");
        return keySerialization != null
                && "JsonRowDataSerializationSchema".equals(keySerialization.getClass().getSimpleName())
                && Boolean.FALSE.equals(ReflectionUtils.retrievePrivateField(recordSerializer, "hasMetadata"))
                && ReflectionUtils.retrievePrivateField(recordSerializer, "partitioner") == null
                && validateUpsertKafkaSerialization(keySerialization)
                && validateUpsertKafkaSerialization(valueSerialization)
                && validateUpsertKafkaKey(toJsonMap(description), "physicalTypes");
    }

    private static boolean validateUpsertKafkaSerialization(Object schema) {
        Object timestampFormat = ReflectionUtils.retrievePrivateField(schema, "timestampFormat");
        return "SQL".equals(String.valueOf(timestampFormat))
                && Boolean.FALSE.equals(ReflectionUtils.retrievePrivateField(schema, "encodeDecimalAsPlainNumber"));
    }

    private static boolean validateSource(String operatorDescription, StreamOperatorFactory operatorFactory) {
        if (!isSourceSupportNative) {
            return false;
        }
        // VALUES-based sources (SimpleInputFormatOperatorFactory): the description is injected by
        // CommonExecValues during planning (format=values, tuples, outputTypes, outputNames, batch).
        // They are executed natively only when the description is complete and every output type is
        // within the native VALUES whitelist; otherwise fall back to Flink.
        if (operatorFactory instanceof SimpleInputFormatOperatorFactory) {
            Map<String, Object> jsonMap = toJsonMap(operatorDescription);
            return jsonMap != null && new ValidateValuesOPStrategy().executeValidateOperator(jsonMap);
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
        Object watermarkStrategy = jsonMap.get("watermarkStrategy");
        if (!(watermarkStrategy instanceof String) || ((String) watermarkStrategy).isEmpty()) {
            return false;
        }
        return !Boolean.TRUE.equals(jsonMap.get("upsertMode"))
                || validateUpsertKafkaKey(jsonMap, "outputTypes");
    }

    /**
     * upsert-kafka 主键校验，source 与 sink 共享同一契约：原生 upsert-kafka source
     * {@code DynamicKafkaDeserializationSchema} 与 sink {@code DynamicKafkaRecordSerializationSchema}
     * 均支持复合主键及白名单内非 BIGINT 主键列类型，因此两侧统一收敛到
     * {@link ValidateChangelogNormalizeOPStrategy#supportsGroupKeyColumns}（与 ChangelogNormalize
     * 分组键语义共享唯一来源），避免两侧契约分叉。
     *
     * <p>包可见以便同包单测覆盖（复合键 / 单键各一条校验用例）。
     */
    static boolean validateUpsertKafkaKey(Map<String, Object> description, String fieldTypes) {
        Object keyProjection = description.get("keyProjection");
        Object rowTypes = description.get(fieldTypes);
        if (!(keyProjection instanceof List) || !(rowTypes instanceof List)) {
            return false;
        }
        List<?> keys = (List<?>) keyProjection;
        List<?> types = (List<?>) rowTypes;
        return ValidateChangelogNormalizeOPStrategy.supportsGroupKeyColumns(types, keys);
    }

    /**
     * Checks whether the chained node list contains a VALUES-based source
     * (SimpleInputFormatOperatorFactory, i.e. the StreamExecValues InputFormat source).
     */
    private static boolean containsValuesSource(List<StreamNode> chainedNode) {
        for (StreamNode node : chainedNode) {
            if (node.getOperatorFactory() instanceof SimpleInputFormatOperatorFactory) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether any operator in the chained node list declares a BINARY/VARBINARY output type.
     *
     * <p>Flink restructures VALUES rows containing non-literal expressions (e.g. CAST) into a
     * LogicalValues.createOneRow seed plus Calc projections, so the BINARY/VARBINARY columns may
     * live in the downstream Calc nodes rather than in the VALUES source itself. The native Calc
     * codegen cannot evaluate BINARY/VARBINARY literals (would hang), so such a chain must fall
     * back to Flink.
     */
    private static boolean chainContainsBinaryOrVarbinaryOutput(List<StreamNode> chainedNode) {
        for (StreamNode node : chainedNode) {
            String operatorDescription = node.getOperatorDescription();
            if (operatorDescription == null || operatorDescription.trim().isEmpty()) {
                continue;
            }
            Map<String, Object> jsonMap = toJsonMap(operatorDescription);
            if (jsonMap == null || !(jsonMap.get("outputTypes") instanceof List)) {
                continue;
            }
            for (Object typeObj : (List<?>) jsonMap.get("outputTypes")) {
                if (!(typeObj instanceof String)) {
                    continue;
                }
                String type = AbstractValidateOperatorStrategy.stripNotNull((String) typeObj).trim();
                if (type.startsWith("BINARY") || type.startsWith("VARBINARY")) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks whether the whole job contains a VALUES-driven chain with BINARY/VARBINARY output.
     *
     * <p>Such a job would otherwise run in Java→native mixed mode (VALUES vertex falls back while
     * the Sink stays native), where the native Java-RowData→VectorBatch conversion
     * (OmniAbstractStreamTaskNetworkInput::createOutputBatch) is incomplete and crashes. The whole
     * job must fall back to Flink to produce correct output.
     */
    public static boolean isJobContainsValuesBinaryChain(
            Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chainInfos) {
        for (StreamingJobGraphGenerator.OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamNode> chainedNode = chainInfo.getAllChainedNodes();
            if (containsValuesSource(chainedNode) && chainContainsBinaryOrVarbinaryOutput(chainedNode)) {
                return true;
            }
        }
        return false;
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
        boolean onlySourceAndSink = true;
        List<String> inputTypeList = new ArrayList<>();
        for (StreamingJobGraphGenerator.OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamNode> chainedNode = chainInfo.getAllChainedNodes();
            for (int i = chainedNode.size() - 1; i >= 0; i--) {

                StreamNode node = chainedNode.get(i);
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
                        return false;
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
