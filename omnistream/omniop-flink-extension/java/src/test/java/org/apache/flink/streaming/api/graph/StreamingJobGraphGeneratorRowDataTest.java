/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
package org.apache.flink.streaming.api.graph;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


class StreamingJobGraphGeneratorRowDataTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROJECTION = "{\"indices\":[{\"exprType\":\"FIELD_REFERENCE\"}],\"condition\":null}";

    private int nextId;

    private StreamNode node(String name, String description) {
        StreamNode node = new StreamNode(nextId++, null, null, (StreamOperatorFactory<?>) null, name, null);
        node.setOperatorDescription(description);
        return node;
    }

    private void connect(StreamNode source, StreamNode target, int input) {
        StreamEdge edge = new StreamEdge(source, target, input, new ForwardPartitioner<>(), null);
        source.addOutEdge(edge);
        target.addInEdge(edge);
    }

    private StreamGraph graph(StreamNode source, StreamNode... consumers) {
        Map<Integer, StreamNode> nodes = new LinkedHashMap<>();
        nodes.put(source.getId(), source);
        for (StreamNode consumer : consumers) {
            nodes.put(consumer.getId(), consumer);
        }
        return new StreamGraph(new ExecutionConfig(), new CheckpointConfig(), SavepointRestoreSettings.none()) {
            @Override
            public Collection<StreamNode> getStreamNodes() {
                return nodes.values();
            }

            @Override
            public StreamNode getStreamNode(Integer id) {
                return nodes.get(id);
            }
        };
    }

    private JsonNode description(StreamNode node) throws Exception {
        return MAPPER.readTree(node.getOperatorDescription());
    }

    @Test
    void projectionConsumesRowsButKeepsVectorBatchOutput() throws Exception {
        StreamNode window = node("GlobalWindowAggregate[1]", "{\"originDescription\":\"keep\"}");
        StreamNode calc = node("Calc[2]", PROJECTION);
        StreamNode join = node("WindowJoin[3]", "{}");
        connect(window, calc, 0);
        connect(calc, join, 1);
        StreamingJobGraphGenerator.applyOmniStreamRowDataConfig(graph(window, calc, join));
        assertEquals("BINARY_ROW_DATA", description(window).get("OutputRecordType").asText());
        assertEquals("keep", description(window).get("originDescription").asText());
        assertEquals("BINARY_ROW_DATA", description(calc).get("InputRecordType").asText());
        assertFalse(description(calc).has("OutputRecordType"));
        assertFalse(description(join).has("InputRecordType1"));
    }

    @Test
    void directJoinMarksTheMatchingInputOnly() throws Exception {
        for (int input = 1; input <= 2; ++input) {
            StreamNode window = node("GlobalWindowAggregate[1]", "{}");
            StreamNode join = node("WindowJoin[2]", "{}");
            connect(window, join, input);
            StreamingJobGraphGenerator.applyOmniStreamRowDataConfig(graph(window, join));
            assertEquals("BINARY_ROW_DATA", description(join).get("InputRecordType" + input).asText());
            assertFalse(description(join).has("InputRecordType" + (3 - input)));
        }
    }

    @Test
    void filteredProjectionKeepsBothDescriptionsUnchanged() {
        StreamNode window = node("GlobalWindowAggregate[1]", "{}");
        String filtered = PROJECTION.replace("null", "{\"exprType\":\"BINARY\"}");
        StreamNode calc = node("Calc[2]", filtered);
        connect(window, calc, 0);
        StreamingJobGraphGenerator.applyOmniStreamRowDataConfig(graph(window, calc));
        assertEquals("{}", window.getOperatorDescription());
        assertEquals(filtered, calc.getOperatorDescription());
    }

    @Test
    void invalidDescriptionsArePreservedWithoutPartialConfiguration() {
        for (String invalid : Arrays.asList("plain description", "{broken", "", "null", "[]")) {
            StreamNode window = node("GlobalWindowAggregate[1]", "{}");
            StreamNode join = node("WindowJoin[2]", invalid);
            connect(window, join, 1);
            StreamingJobGraphGenerator.applyOmniStreamRowDataConfig(graph(window, join));
            assertEquals("{}", window.getOperatorDescription());
            assertEquals(invalid, join.getOperatorDescription());
        }
    }

    @Test
    void unsupportedFanoutKeepsAllConsumersOnVectorBatches() {
        StreamNode window = node("GlobalWindowAggregate[1]", "{}");
        StreamNode join = node("WindowJoin[2]", "{}");
        StreamNode other = node("GroupAggregate[3]", "{}");
        connect(window, join, 1);
        connect(window, other, 0);
        StreamingJobGraphGenerator.applyOmniStreamRowDataConfig(graph(window, join, other));
        assertEquals("{}", window.getOperatorDescription());
        assertEquals("{}", join.getOperatorDescription());
    }

    @Test
    void invalidJoinInputDoesNotConfigureTheProducer() {
        StreamNode window = node("GlobalWindowAggregate[1]", "{}");
        StreamNode join = node("WindowJoin[2]", "{}");
        connect(window, join, 0);
        StreamingJobGraphGenerator.applyOmniStreamRowDataConfig(graph(window, join));
        assertEquals("{}", window.getOperatorDescription());
        assertEquals("{}", join.getOperatorDescription());
    }
}
