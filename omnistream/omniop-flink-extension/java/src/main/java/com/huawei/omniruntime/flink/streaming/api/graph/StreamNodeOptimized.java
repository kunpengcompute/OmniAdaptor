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

import com.google.gson.Gson;

import com.huawei.omniruntime.flink.utils.ReflectionUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.client.cli.UdfConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

public class StreamNodeOptimized implements StreamNodeExtraDescription {

    private static final Logger LOG = LoggerFactory.getLogger(StreamNodeOptimized.class);
    private static final StreamNodeOptimized INSTANCE = new StreamNodeOptimized();

    private static final String FEATURE_SO_NAME = "udf_so";
    private static final String FEATURE_KEY_BY_NAME = "key_so"; // value is array, maybe 0, 1, 2...
    private static final String FEATURE_SHUFFLE_NAME = "hash_so";
    private static final String FEATURE_UDF_OBJ = "udf_obj";

    private static final Set<String> SUPPORT_KAFKA_DESERIALIZATION_SCHEMA_TPYE = new HashSet<>();
    private static final Set<String> SUPPORT_KAFKA_SERIALIZATION_SCHEMA_TPYE = new HashSet<>();

    static {
        SUPPORT_KAFKA_DESERIALIZATION_SCHEMA_TPYE.addAll(Arrays.asList("SimpleStringSchema"));
        SUPPORT_KAFKA_SERIALIZATION_SCHEMA_TPYE.addAll(Arrays.asList("SimpleStringSchema"));
    }

    static StreamNodeOptimized getInstance() {
        return INSTANCE;
    }

    @SuppressWarnings("unchecked")
    public boolean setExtraDescription(StreamNode streamNode, StreamConfig streamConfig, StreamGraph streamGraph, JobType jobType) throws NoSuchFieldException, IllegalAccessException, IOException, ClassNotFoundException {
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        Map<String, Object> jsonMap = new LinkedHashMap<>();

        List<Map<String, Object>> inputTypes = toStringTypeSerializers(streamNode.getTypeSerializersIn());
        Map<String, Object> outputTypes = toStringTypeSerializer(streamNode.getTypeSerializerOut());
        Map<String, Object> stateKeyTypes = toStringTypeSerializer(streamNode.getStateKeySerializer());

        if (streamNode.getOperatorFactory() instanceof SimpleUdfStreamOperatorFactory) {
            StreamOperator<?> operator = streamNode.getOperator();
            if (operator instanceof AbstractUdfStreamOperator) {
                Function udf = ((AbstractUdfStreamOperator<?, ?>) operator).getUserFunction();
                // udf->json
                // Java的transient关键字原本是用于阻止默认的Java序列化机制（比如ObjectOutputStream）序列化字段。Jackson默认不遵循这个关键字，但Gson默认会尊重
                try {
                    Gson gson = new Gson();
                    String udfObj = gson.toJson(udf);
                    jsonMap.put(FEATURE_UDF_OBJ, udfObj);
                } catch (UnsupportedOperationException e) {
                    LOG.error(e.getMessage());
                    return false;
                }

                if (!setUdfInfo((AbstractUdfStreamOperator) operator, jsonMap)) {
                    return false;
                }
            }

            // Check whether the downstream operator needs to perform hash shuffle.
            // If yes, extend the SO path of the downstream keyselector.
            Map<Integer, Object> shuffleMap = new LinkedHashMap<>();
            for (StreamEdge edge : streamNode.getOutEdges()) {
                if (edge.getPartitioner().toString().equals("HASH")) {
                    StreamNode nextNode = streamGraph.getStreamNode(edge.getTargetId());
                    setHashSelector(nextNode.getStatePartitioners(), edge.getTargetId(), shuffleMap);
                }
            }
            jsonMap.put(FEATURE_SHUFFLE_NAME, shuffleMap);

            if (!setKeySelector(streamNode.getStatePartitioners(), jsonMap)) {
                return false;
            }
        }

        if (streamNode.getOperatorFactory() instanceof SinkWriterOperatorFactory) {
            Sink sink = ((SinkWriterOperatorFactory<?, ?>) streamNode.getOperatorFactory()).getSink();
            if (!"KafkaSink".equals(sink.getClass().getSimpleName()) || !validateSinkAndSetDesc(sink, jsonMap)) {
                return false;
            }
        }

        if (streamNode.getOperatorFactory() instanceof SourceOperatorFactory) {
            if (validateSourceAndSetDesc(streamNode, jsonMap)) {
                return false;
            }
        }

        jsonMap.put("inputTypes", inputTypes);
        jsonMap.put("outputTypes", outputTypes);
        jsonMap.put("stateKeyTypes", stateKeyTypes);
        jsonMap.put("index", streamNode.getId());
        jsonMap.put("jobType", jobType.getValue()); // no longer in use
        jsonMap.put("originDescription", streamConfig.getDescription());
        String jsonString = "";
        try {
            jsonString = objectMapper.writeValueAsString(jsonMap);
        } catch (Exception e) {
            LOG.error("Error serializing JSON", e);  // Handle the exception or log it
            return false;
        }
        streamConfig.setDescription(jsonString);
        return true;
    }

    private boolean validateSourceAndSetDesc(StreamNode streamNode, Map<String, Object> jsonMap) {
        SourceOperatorFactory<?> sourceOperatorFactory = (SourceOperatorFactory<?>) streamNode.getOperatorFactory();
        Source source = ReflectionUtils.retrievePrivateField(sourceOperatorFactory, "source");
        if (!"KafkaSource".equals(source.getClass().getSimpleName())) {
            return true;
        }
        jsonMap.put("batch", false);
        jsonMap.put("format", "kafka");
        Optional<String> deserializationSchema = getDeserializationSchema(source);
        if (!deserializationSchema.isPresent() || !SUPPORT_KAFKA_DESERIALIZATION_SCHEMA_TPYE.contains(deserializationSchema.get())) {
            return true;
        }
        jsonMap.put("deserializationSchema", deserializationSchema.get());

        WatermarkStrategy watermarkStrategy = ReflectionUtils.retrievePrivateField(
            sourceOperatorFactory, "watermarkStrategy");
        WatermarkGenerator watermarkGenerator = watermarkStrategy.createWatermarkGenerator(null);
        if (watermarkGenerator instanceof NoWatermarksGenerator) {
            jsonMap.put("watermarkStrategy", "no");
        } else if (watermarkGenerator instanceof AscendingTimestampsWatermarks) {
            jsonMap.put("watermarkStrategy", "ascending");
        } else if (watermarkGenerator instanceof BoundedOutOfOrdernessWatermarks) {
            jsonMap.put("watermarkStrategy", "bounded");
            long outOfOrdernessMillis = ReflectionUtils.retrievePrivateField(
                watermarkGenerator, "outOfOrdernessMillis");
            jsonMap.put("outOfOrdernessMillis", outOfOrdernessMillis);
        } else {
            LOG.warn("Unsupported watermark strategy: "
                + watermarkGenerator.getClass().getName());
            return true;
        }
        Properties props = ReflectionUtils.retrievePrivateField(source, "props");
        Map<String, String> properties = new HashMap<>();
        properties.put("group.id", "omni-group");
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            properties.put(entry.getKey().toString(), entry.getValue().toString());
        }
        jsonMap.put("properties", properties);
        boolean emitProgressiveWatermarks = ReflectionUtils.retrievePrivateField(
            sourceOperatorFactory, "emitProgressiveWatermarks");
        jsonMap.put("emitProgressiveWatermarks", emitProgressiveWatermarks);
        return false;
    }

    private Optional<String> getDeserializationSchema(Source source) {
        if (!source.getClass().getSimpleName().equals("KafkaSource")) {
            return Optional.empty();
        }
        Object kafkaRecordDeserializationSchema = ReflectionUtils.retrievePrivateField(source, "deserializationSchema");
        if (kafkaRecordDeserializationSchema.getClass().getSimpleName().equals("KafkaValueOnlyDeserializerWrapper")) {
            return Optional.empty();
        }
        if (kafkaRecordDeserializationSchema.getClass().getSimpleName().equals("KafkaValueOnlyDeserializationSchemaWrapper")) {
            return Optional.of(ReflectionUtils.retrievePrivateField(
                kafkaRecordDeserializationSchema, "deserializationSchema").getClass().getSimpleName());
        }
        if (!kafkaRecordDeserializationSchema.getClass().getSimpleName().equals("KafkaDeserializationSchemaWrapper")) {
            return Optional.empty();
        }

        Object kafkaDeserializationSchema = ReflectionUtils.retrievePrivateField(kafkaRecordDeserializationSchema, "kafkaDeserializationSchema");
        if (kafkaDeserializationSchema.getClass().getName().equals("org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper")) {
            return Optional.of(ReflectionUtils.retrievePrivateField(
                kafkaDeserializationSchema, "deserializationSchema").getClass().getSimpleName());
        }

        if (!kafkaDeserializationSchema.getClass().getSimpleName().equals("DynamicKafkaDeserializationSchema")) {
            return Optional.empty();
        }
        Object keyDeserialization = ReflectionUtils.retrievePrivateField(kafkaDeserializationSchema, "keyDeserialization");
        if (keyDeserialization != null) {
            return Optional.empty();
        }
        return Optional.of(ReflectionUtils.retrievePrivateField(kafkaDeserializationSchema, "valueDeserialization").getClass().getSimpleName());
    }

    private List<Map<String, Object>> toStringTypeSerializers(TypeSerializer<?>[] TypeSerializers) {
        List<Map<String, Object>> typeList = new ArrayList<>();
        for (TypeSerializer<?> inputType : TypeSerializers) {
            typeList.add(toStringTypeSerializer(inputType));
        }
        return typeList;
    }

    private Map<String, Object> toStringTypeSerializer(TypeSerializer<?> inputType) {
        Map<String, Object> inputTypeJson = new LinkedHashMap<>();
        if (inputType == null) {
            return inputTypeJson;
        }
        String typeName = inputType.getClass().getName();
        inputTypeJson.put("serializerName", typeName);
        List<Map<String, Object>> fieldSerializers = null;
        if (inputType instanceof TupleSerializer) {
            fieldSerializers = toStringTypeSerializers(((TupleSerializer<?>) inputType).getFieldSerializers());
            inputTypeJson.put("fieldSerializers", fieldSerializers);
        }
        if (inputType instanceof PojoSerializer) {
            fieldSerializers = toStringTypeSerializers(ReflectionUtils.retrievePrivateField(inputType, "fieldSerializers"));
            List<String> fields = getFieldsName(ReflectionUtils.retrievePrivateField(inputType, "fields"));
            inputTypeJson.put("fields", fields);
            inputTypeJson.put("clazz", ((Class<?>)ReflectionUtils.retrievePrivateField(inputType, "clazz")).getName().replaceAll("\\.", "_"));
            inputTypeJson.put("fieldSerializers", fieldSerializers);
        }
        if (inputType instanceof MapSerializer) {
            Map<String, Object> keySerializer = toStringTypeSerializer(((MapSerializer<?, ?>) inputType).getKeySerializer());
            Map<String, Object> valueSerializer = toStringTypeSerializer(((MapSerializer<?, ?>) inputType).getValueSerializer());
            inputTypeJson.put("keySerializer", keySerializer);
            inputTypeJson.put("valueSerializer", valueSerializer);
        }
        if (inputType instanceof ListSerializer) {
            TypeSerializer<?> elementSerializer = ReflectionUtils.retrievePrivateField(inputType, "elementSerializer");
            inputTypeJson.put("elementSerializer", toStringTypeSerializer(elementSerializer));
        }
        return inputTypeJson;
    }

    private List<String> getFieldsName(Field[] fields) {
        List<String> fieldName = new ArrayList<>();
        for (Field field : fields) {
            fieldName.add(field.getName());
        }
        return fieldName;
    }

    private boolean setUdfInfo(AbstractUdfStreamOperator operator, Map<String, Object> jsonMap) {
        Function udf = operator.getUserFunction();
        String udfName = parseUdfName(udf);
        if (udfName.isEmpty()) {
            return false;
        }
        Properties config = UdfConfig.getINSTANCE().getConfig();
        String udfSo = "";
        for (String stringPropertyName : config.stringPropertyNames()) {
            if (udfName.equals(stringPropertyName)) {
                udfSo = config.getProperty(stringPropertyName);
                break;
            }
        }
        if (udfSo.isEmpty()) {
            return false;
        }

        jsonMap.put(FEATURE_SO_NAME, udfSo);
        return true;
    }

    private boolean setHashSelector(KeySelector<?, ?>[] statePartitioners, Integer targetId, Map<Integer, Object> shuffleMap) {
        // The multi-output scenario is not considered.
        if (statePartitioners.length > 1) {
            return false;
        }
        ArrayList<String> Keys = new ArrayList<>();
        for (KeySelector<?, ?> statePartitioner : statePartitioners) {
            String name = parseUdfName(statePartitioner);
            if (name.isEmpty()) {
                return false;
            }
            Properties config = UdfConfig.getINSTANCE().getConfig();
            boolean isMatch = false;
            for (String stringPropertyName : config.stringPropertyNames()) {
                if (name.equals(stringPropertyName)) {
                    Keys.add(config.getProperty(stringPropertyName));
                    isMatch = true;
                    break;
                }
            }
            if (!isMatch) {
                return false;
            }
        }
        shuffleMap.put(targetId, Keys.isEmpty() ? "" : Keys.get(0));
        return true;
    }

    private boolean setKeySelector(KeySelector<?, ?>[] statePartitioners, Map<String, Object> jsonMap) {
        ArrayList<String> Keys = new ArrayList<>();
        Properties config = UdfConfig.getINSTANCE().getConfig();
        for (KeySelector<?, ?> statePartitioner : statePartitioners) {
            String name = parseUdfName(statePartitioner);
            if (name.isEmpty()) {
                return false;
            }
            boolean isMatch = false;
            for (String stringPropertyName : config.stringPropertyNames()) {
                if (name.equals(stringPropertyName)) {
                    Keys.add(config.getProperty(stringPropertyName));
                    break;
                }
            }
        }
        jsonMap.put(FEATURE_KEY_BY_NAME, Keys);
        return true;
    }

    private String parseUdfName(Object object) {
        String name = object.getClass().getName();
        if (name.contains("$Lambda")) {
            SerializedLambda lambda = extract(object);
            if (lambda == null) {
                return "";
            }
            // implClass:The class where the implementation method resides.
            // implMethodName:The name of the implementation method.
            // functionalInterfaceClass: Functional interface implemented with lambda.
            String implClass = ReflectionUtils.retrievePrivateField(lambda, "implClass");
            String implMethodName = ReflectionUtils.retrievePrivateField(lambda, "implMethodName");
            String functionalInterfaceClass = ReflectionUtils.retrievePrivateField(lambda, "functionalInterfaceClass");
            name = implClass.replaceAll("/", ".") + "$" + implMethodName + "#" + functionalInterfaceClass.replaceAll("/", ".");
        }
        return name;
    }

    private SerializedLambda extract(Object lambda) {
        if (!lambda.getClass().isSynthetic()) {
            LOG.error("{} is not lambda.", lambda);
            return null;
        }
        try {
            // If a class implements Serializable, there will be a writeReplace method that returns a SerializedLambda.
            // SerializedLambda is used to describe the structural information of this lambda.
            Method writeReplace = lambda.getClass().getDeclaredMethod("writeReplace");
            writeReplace.setAccessible(true);
            return (SerializedLambda) writeReplace.invoke(lambda);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            LOG.error("{} is not lambda.{}", lambda, e.getMessage());
            return null;
        }
    }

    private boolean validateSinkAndSetDesc(Sink sink, Map<String, Object> jsonMap) {
        try {
            jsonMap.put("batch", false);
            DeliveryGuarantee deliveryGuarantee = ReflectionUtils.retrievePrivateField(sink, "deliveryGuarantee");
            jsonMap.put("deliveryGuarantee", deliveryGuarantee.name());

            String transactionalIdPrefix = ReflectionUtils.retrievePrivateField(sink, "transactionalIdPrefix");
            jsonMap.put("transactionalIdPrefix", transactionalIdPrefix);

            Properties kafkaProducerConfig = ReflectionUtils.retrievePrivateField(sink, "kafkaProducerConfig");
            Map<String, String> properties = new HashMap<>();
            for (Map.Entry<Object, Object> entry : kafkaProducerConfig.entrySet()) {
                properties.put(entry.getKey().toString(), entry.getValue().toString());
            }
            jsonMap.put("kafkaProducerConfig", properties);
            Object recordSerializer = ReflectionUtils.retrievePrivateField(sink, "recordSerializer");
            if (!"KafkaRecordSerializationSchemaWrapper".equals(recordSerializer.getClass().getSimpleName())) {
                return false;
            }
            Object topicSelector = ReflectionUtils.retrievePrivateField(recordSerializer, "topicSelector");
            String topic = ((java.util.function.Function<?, String>) topicSelector).apply(null);
            jsonMap.put("topic", topic);
            Object valueSerializationSchema = ReflectionUtils.retrievePrivateField(recordSerializer, "valueSerializationSchema");
            return "SimpleStringSchema".equals(valueSerializationSchema.getClass().getSimpleName());
        } catch (Exception e) {
            return false;
        }
    }

    private Optional<String> getSinkTopic(Sink sink) {
        Object recordSerializer = ReflectionUtils.retrievePrivateField(sink, "recordSerializer");
        if (!"KafkaRecordSerializationSchemaWrapper".equals(recordSerializer.getClass().getSimpleName())) {
            return Optional.empty();
        }
        Object topicSelector = ReflectionUtils.retrievePrivateField(recordSerializer, "topicSelector");
        String topic = ((java.util.function.Function<?, String>) topicSelector).apply(null);
        return Optional.of(topic);
    }
}
