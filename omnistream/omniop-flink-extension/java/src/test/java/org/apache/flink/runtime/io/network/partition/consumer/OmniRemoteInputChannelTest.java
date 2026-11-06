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
 */

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OmniRemoteInputChannelTest {
    private OmniRemoteInputChannel channel(SingleInputGate gate) {
        return new OmniRemoteInputChannel(gate, 0, new ResultPartitionID(), 0,
                mock(ConnectionID.class), mock(ConnectionManager.class), 0, 0, 0,
                mock(Counter.class), mock(Counter.class), mock(ChannelStateWriter.class));
    }

    @Test
    void ordinaryJavaConsumersKeepGateNotifications() {
        SingleInputGate gate = mock(SingleInputGate.class, RETURNS_DEEP_STUBS);
        OmniRemoteInputChannel channel = channel(gate);
        channel.notifyChannelNonEmpty();
        channel.notifyPriorityEvent(7);
        verify(gate).notifyChannelNonEmpty(channel);
        verify(gate).notifyPriorityEvent(channel, 7);
    }

    @Test
    void nativeConsumerReceivesDataPriorityAndErrorNotifications() {
        SingleInputGate gate = mock(SingleInputGate.class, RETURNS_DEEP_STUBS);
        OmniRemoteInputChannel channel = channel(gate);
        AtomicInteger notifications = new AtomicInteger();
        channel.setDataAvailableListener(notifications::incrementAndGet);
        assertEquals(1, notifications.get());
        channel.notifyChannelNonEmpty();
        channel.notifyPriorityEvent(7);
        channel.onError(new IOException("network failure"));
        assertEquals(4, notifications.get());
        verify(gate, never()).notifyChannelNonEmpty(channel);
        verify(gate, never()).notifyPriorityEvent(channel, 7);
        channel.setDataAvailableListener(null);
        channel.notifyChannelNonEmpty();
        assertEquals(4, notifications.get());
        verify(gate).notifyChannelNonEmpty(channel);
    }

    @Test
    void directNativeConsumptionChecksAsynchronousChannelErrors() {
        OmniRemoteInputChannel channel = channel(mock(SingleInputGate.class, RETURNS_DEEP_STUBS));
        IOException failure = new IOException("connection lost");
        channel.onError(failure);
        assertSame(failure, assertThrows(IOException.class, channel::getNextBuffer));
    }

    @Test
    void recoveryConversionCreatesNotifyingChannel() throws IOException {
        SingleInputGate gate = mock(SingleInputGate.class, RETURNS_DEEP_STUBS);
        RemoteRecoveredInputChannel recovered = new RemoteRecoveredInputChannel(gate, 0,
                new ResultPartitionID(), 0, mock(ConnectionID.class), mock(ConnectionManager.class),
                0, 0, 0, mock(InputChannelMetrics.class, RETURNS_DEEP_STUBS));
        recovered.setChannelStateWriter(mock(ChannelStateWriter.class));
        assertInstanceOf(OmniRemoteInputChannel.class, recovered.toInputChannelInternal());
    }
}
