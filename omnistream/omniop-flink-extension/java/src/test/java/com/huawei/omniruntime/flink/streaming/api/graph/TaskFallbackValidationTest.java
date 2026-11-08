package com.huawei.omniruntime.flink.streaming.api.graph;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskFallbackValidationTest {
    @Test
    public void supportedSourceIsNotForcedToFlink() {
        Set<Integer> endpoints = new HashSet<>();
        assertTrue(validate(endpoints, node(1, "Source: supported", "{}")));
        assertTrue(endpoints.isEmpty());
    }

    @Test
    public void endpointNamesRequireMatchingTopology() {
        StreamNode sink = node(2, "Sink: unsupported", "{}");
        // An internal edge makes this source-named operator ineligible as an input boundary.
        StreamNode source = node(1, "Source: unsupported", "{}");
        assertTrue(OmniGraphOverride.isFallbackEndpoint(source));
        assertTrue(OmniGraphOverride.isFallbackEndpoint(sink));
        when(source.getInEdges()).thenReturn(Collections.singletonList(mock(
                org.apache.flink.streaming.api.graph.StreamEdge.class)));
        when(sink.getOutEdges()).thenReturn(Collections.singletonList(mock(
                org.apache.flink.streaming.api.graph.StreamEdge.class)));
        assertFalse(OmniGraphOverride.isFallbackEndpoint(source));
        assertFalse(OmniGraphOverride.isFallbackEndpoint(sink));
    }

    @Test
    public void rejectedValuesSourceIsReportedAsEndpointFallback() {
        StreamNode source = node(1, "Source: values", "{}");
        when(source.getOperatorFactory()).thenReturn(mock(
                org.apache.flink.streaming.api.operators.SimpleInputFormatOperatorFactory.class));
        Set<Integer> endpoints = new HashSet<>();
        assertFalse(validate(endpoints, source));
        assertTrue(endpoints.contains(1));
    }

    @Test
    public void unsupportedMiddleInSourceChainRequiresWholeJobFallback() {
        Set<Integer> endpoints = new HashSet<>();
        assertFalse(validate(endpoints, node(1, "Source: supported", "{}"),
                node(2, "UnsupportedMiddleOperator", "{}")));
        assertTrue(endpoints.isEmpty());
    }

    @Test
    public void rejectedSourceCannotHideRejectedMiddleInSameChain() {
        StreamNode source = node(1, "Source: values", "{}");
        when(source.getOperatorFactory()).thenReturn(mock(
                org.apache.flink.streaming.api.operators.SimpleInputFormatOperatorFactory.class));
        Set<Integer> endpoints = new HashSet<>();
        assertFalse(validate(endpoints, source, node(2, "UnsupportedMiddleOperator", "{}")));
        assertTrue(endpoints.isEmpty());
    }

    @Test
    public void writerAndEndInSameTaskFormFallbackSinkStages() {
        StreamNode writer = node(1, "StreamingFileWriter", "{}");
        StreamNode end = node(2, "Sink: end", "{}");
        connect(writer, end);
        assertTrue(sinkStages(writer, end).containsAll(Arrays.asList(1, 2)));
    }

    @Test
    public void writerAndGlobalCommitterAcrossTasksBothFallBack() {
        StreamNode writer = node(1, "Writer : paimon", "{}");
        StreamNode committer = node(2, "Global Committer : paimon", "{}");
        StreamNode end = node(3, "end: Writer", "{}");
        connect(writer, committer);
        connect(committer, end);
        Map<Integer, StreamingJobGraphGenerator.OperatorChainInfo> chains = new HashMap<>();
        chains.put(1, chain(writer));
        chains.put(2, chain(committer, end));
        assertTrue(OmniGraphOverride.collectSinkFallbackNodes(chains)
                .containsAll(Arrays.asList(1, 2, 3)));
    }

    @Test
    public void writerFollowedByBusinessComputationIsNotASinkBoundary() {
        StreamNode writer = node(1, "BusinessWriter", "{}");
        StreamNode calc = node(2, "Calc", "{}");
        StreamNode sink = node(3, "Sink: end", "{}");
        connect(writer, calc);
        connect(calc, sink);
        assertTrue(sinkStages(writer, calc, sink).isEmpty());
    }

    @Test
    public void ordinaryMapBeforeWriterIsStillMiddleComputation() {
        StreamNode map = node(1, "Map", "{}");
        StreamNode writer = node(2, "Writer : paimon", "{}");
        connect(map, writer);
        Set<Integer> stages = sinkStages(map, writer);
        assertFalse(stages.contains(1));
        assertTrue(stages.contains(2));
    }

    @Test
    public void writerIsForcedToFlinkWithoutForcingOrdinarySupportedSink() {
        Set<Integer> endpoints = new HashSet<>();
        assertFalse(validate(endpoints, node(1, "StreamingFileWriter", "{}")));
        assertTrue(endpoints.contains(1));
        endpoints.clear();
        assertTrue(validate(endpoints, node(1, "Sink: supported", "{}")));
        assertTrue(endpoints.isEmpty());
    }

    @Test
    public void rejectedMiddleBesideWriterStillRequiresWholeJobFallback() {
        StreamNode middle = node(2, "UnsupportedMiddleOperator", "{}");
        StreamNode writer = node(1, "Writer : output", "{}");
        connect(middle, writer);
        Set<Integer> endpoints = new HashSet<>();
        assertFalse(validate(endpoints, writer, middle));
        assertTrue(endpoints.isEmpty());
    }

    @Test
    public void cyclicWriterStagesCannotBeAcceptedAsTerminalSink() {
        StreamNode first = node(1, "Writer first", "{}");
        StreamNode second = node(2, "Writer second", "{}");
        connect(first, second);
        connect(second, first);
        assertTrue(sinkStages(first, second).isEmpty());
    }

    private static Set<Integer> sinkStages(StreamNode... nodes) {
        return OmniGraphOverride.collectSinkFallbackNodes(Collections.singletonMap(1, chain(nodes)));
    }

    private static StreamingJobGraphGenerator.OperatorChainInfo chain(StreamNode... nodes) {
        StreamingJobGraphGenerator.OperatorChainInfo chain =
                mock(StreamingJobGraphGenerator.OperatorChainInfo.class);
        when(chain.getAllChainedNodes()).thenReturn(Arrays.asList(nodes));
        return chain;
    }

    private static void connect(StreamNode from, StreamNode to) {
        StreamEdge edge = mock(StreamEdge.class);
        when(edge.getTargetId()).thenReturn(to.getId());
        when(from.getOutEdges()).thenReturn(Collections.singletonList(edge));
        when(to.getInEdges()).thenReturn(Collections.singletonList(edge));
    }

    private static boolean validate(Set<Integer> endpoints, StreamNode... nodes) {
        JobVertex vertex = new JobVertex("test-chain");
        StreamGraph graph = mock(StreamGraph.class);
        when(graph.getStreamEdges(org.mockito.ArgumentMatchers.anyInt()))
                .thenReturn(Collections.emptyList());
        StreamingJobGraphGenerator.OperatorChainInfo chain =
                mock(StreamingJobGraphGenerator.OperatorChainInfo.class);
        when(chain.getAllChainedNodes()).thenReturn(Arrays.asList(nodes));
        when(chain.getStreamGraph()).thenReturn(graph);
        Map<Integer, StreamConfig> configs = new HashMap<>();
        configs.put(1, new StreamConfig(vertex.getConfiguration()));
        return OmniGraphOverride.validateVertexForOmniTask(new AbstractMap.SimpleEntry<>(1, vertex),
                Collections.singletonMap(1, chain), Collections.singletonMap(1, Collections.emptyMap()),
                configs, JobType.SQL, endpoints);
    }

    private static StreamNode node(int id, String name, String description) {
        StreamNode node = mock(StreamNode.class);
        when(node.getId()).thenReturn(id);
        when(node.getOperatorName()).thenReturn(name);
        when(node.getOperatorDescription()).thenReturn(description);
        when(node.getOperatorFactory()).thenReturn(mock(StreamOperatorFactory.class));
        when(node.getInEdges()).thenReturn(Collections.emptyList());
        when(node.getOutEdges()).thenReturn(Collections.emptyList());
        return node;
    }
}
