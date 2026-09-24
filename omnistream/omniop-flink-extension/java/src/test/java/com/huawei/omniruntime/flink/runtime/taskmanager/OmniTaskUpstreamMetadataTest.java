package com.huawei.omniruntime.flink.runtime.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OmniTaskUpstreamMetadataTest {
    @Test
    void preservesGateOrderAndEmptySourceInputs() throws Exception {
        assertEquals(Arrays.asList(true, false, true),
                OmniTask.parseUpstreamNative(" [true, false, true] "));
        assertEquals(Collections.emptyList(), OmniTask.parseUpstreamNative("[]"));
    }

    @Test
    void rejectsMissingMalformedOrUnknownFlags() {
        for (String value : new String[] {null, "", "true", "[yes]", "[true,]", "[,false]", "[TRUE]"}) {
            assertThrows(IOException.class, () -> OmniTask.parseUpstreamNative(value));
        }
    }

    @Test
    void physicalGateIndicesSurviveReorderedGateArrays() throws Exception {
        StreamConfig config = new StreamConfig(new Configuration());
        config.setUpstreamNative("[true, false]");
        IndexedInputGate gate0 = gate(0);
        IndexedInputGate gate1 = gate(1);
        assertEquals(Arrays.asList(true, false),
                OmniTask.readUpstreamNative(config, new IndexedInputGate[] {gate1, gate0}));
    }

    @Test
    void rejectsMissingDuplicateAndOutOfRangeGateMappings() {
        StreamConfig config = new StreamConfig(new Configuration());
        config.setUpstreamNative("[true, false]");
        assertThrows(IOException.class,
                () -> OmniTask.readUpstreamNative(config, new IndexedInputGate[] {gate(0)}));
        assertThrows(IOException.class,
                () -> OmniTask.readUpstreamNative(config, new IndexedInputGate[] {gate(0), gate(0)}));
        assertThrows(IOException.class,
                () -> OmniTask.readUpstreamNative(config, new IndexedInputGate[] {gate(0), gate(2)}));
        assertThrows(IOException.class,
                () -> OmniTask.readUpstreamNative(config, new IndexedInputGate[] {gate(-1), gate(1)}));
    }

    private static IndexedInputGate gate(int index) {
        IndexedInputGate gate = mock(IndexedInputGate.class);
        when(gate.getGateIndex()).thenReturn(index);
        return gate;
    }
}
