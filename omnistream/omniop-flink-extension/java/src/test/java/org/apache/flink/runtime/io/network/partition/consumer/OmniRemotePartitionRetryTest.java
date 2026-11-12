/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.deployment.SubpartitionIndexRange;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

class OmniRemotePartitionRetryTest {
    private SingleInputGate gate() {
        return new SingleInputGate("remote-partition-retry-test", 0,
                new IntermediateDataSetID(), ResultPartitionType.PIPELINED_BOUNDED,
                new SubpartitionIndexRange(0, 0), 1,
                mock(PartitionProducerStateProvider.class), () -> mock(BufferPool.class),
                null, mock(MemorySegmentProvider.class), 32768,
                mock(ThroughputCalculator.class), null);
    }

    private ResultPartitionID install(SingleInputGate gate, RemoteInputChannel channel) {
        ResultPartitionID partition = new ResultPartitionID();
        when(channel.getPartitionId()).thenReturn(partition);
        when(channel.getConsumedSubpartitionIndex()).thenReturn(0);
        gate.setInputChannels(channel);
        return partition;
    }

    @Test
    void omniSubclassIsRetriedByTheRealInputGate() throws Exception {
        SingleInputGate gate = gate();
        OmniRemoteInputChannel channel = mock(OmniRemoteInputChannel.class);
        ResultPartitionID partition = install(gate, channel);
        try {
            // The original exact-class comparison throws before reaching the retry method.
            assertDoesNotThrow(() -> gate.retriggerPartitionRequest(partition.getPartitionId(), 0));
            verify(channel).retriggerSubpartitionRequest();
        } finally {
            gate.close();
        }
    }

    @Test
    void originalRemoteChannelKeepsItsRetryRoute() throws Exception {
        SingleInputGate gate = gate();
        RemoteInputChannel channel = mock(RemoteInputChannel.class);
        ResultPartitionID partition = install(gate, channel);
        try {
            gate.retriggerPartitionRequest(partition.getPartitionId(), 0);
            verify(channel).retriggerSubpartitionRequest();
        } finally {
            gate.close();
        }
    }
}
