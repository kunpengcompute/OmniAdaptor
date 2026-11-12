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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Field;
import java.io.IOException;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OmniReaderAvailabilityTest {
    static class Reader extends OmniCreditBasedSequenceNumberingViewReader {
        int nativeType = -1;
        int nativeBacklog;
        int stops;
        int releases;
        Reader(int credit, PartitionRequestQueue queue) throws Exception {
            super(new InputChannelID(), credit, queue, true);
            setTaskName("remote-reader-availability-test");
            Field ref = OmniCreditBasedSequenceNumberingViewReader.class
                    .getDeclaredField("nativeCreditBasedSequenceNumberingViewReaderRef");
            ref.setAccessible(true);
            ref.setLong(this, 1);
        }
        @Override public int getAvailabilityAndBacklog(long ref, int credit) { return nativeBacklog; }
        @Override protected int getNextBufferType(long ref) { return nativeType; }
        @Override protected void stopNativeReader(long ref) { stops++; }
        @Override protected void releaseNativeViewReader(long ref) { releases++; }
        @SuppressWarnings("unchecked")
        void cache(Buffer.DataType type) throws Exception {
            NetworkBuffer buffer = mock(NetworkBuffer.class);
            when(buffer.getDataType()).thenReturn(type);
            when(buffer.isBuffer()).thenReturn(type.isBuffer());
            Field field = OmniCreditBasedSequenceNumberingViewReader.class.getDeclaredField("bufferInfos");
            field.setAccessible(true);
            ((List<BufferInfo>) field.get(this)).add(new BufferInfo(buffer, 0, 10));
        }
    }

    @Test
    void availabilityRequiresDataAndRespectsCredit() throws Exception {
        Reader reader = new Reader(0, mock(PartitionRequestQueue.class));
        try {
            assertFalse(reader.getAvailabilityAndBacklog().isAvailable());
            reader.nativeType = 0;
            reader.nativeBacklog = 3;
            assertFalse(reader.getAvailabilityAndBacklog().isAvailable());
            assertEquals(3, reader.getAvailabilityAndBacklog().getBacklog());
            reader.addCredit(1);
            assertTrue(reader.getAvailabilityAndBacklog().isAvailable());
        } finally { reader.stop(); }
    }

    @Test
    void eventsCanBeSentWithoutDataCredit() throws Exception {
        Reader reader = new Reader(0, mock(PartitionRequestQueue.class));
        try {
            reader.cache(Buffer.DataType.EVENT_BUFFER);
            assertTrue(reader.getAvailabilityAndBacklog().isAvailable());
            assertNotNull(reader.getNextBuffer());
            assertEquals(0, reader.getNumCreditsAvailable());
        } finally { reader.stop(); }
    }

    @Test
    void nativeBacklogKeepsReaderAvailableAfterJavaCacheIsDrained() throws Exception {
        Reader reader = new Reader(2, mock(PartitionRequestQueue.class));
        try {
            reader.cache(Buffer.DataType.DATA_BUFFER);
            reader.nativeType = 0;
            reader.nativeBacklog = 12;
            InputChannel.BufferAndAvailability buffer = reader.getNextBuffer();
            assertTrue(buffer.moreAvailable());
            assertEquals(12, buffer.buffersInBacklog());
            assertEquals(1, reader.getNumCreditsAvailable());
        } finally { reader.stop(); }
    }

    @Test
    void lastCreditStopsDataSchedulingUntilCreditReturns() throws Exception {
        Reader reader = new Reader(1, mock(PartitionRequestQueue.class));
        try {
            reader.cache(Buffer.DataType.DATA_BUFFER);
            reader.nativeType = 0;
            reader.nativeBacklog = 1;
            InputChannel.BufferAndAvailability lastCredit = reader.getNextBuffer();
            assertFalse(lastCredit.moreAvailable());
            assertEquals(1, lastCredit.buffersInBacklog());
            assertFalse(reader.getAvailabilityAndBacklog().isAvailable());
            reader.addCredit(1);
            assertTrue(reader.getAvailabilityAndBacklog().isAvailable());
        } finally { reader.stop(); }
    }

    @Test
    void failedNativeDrainIsSurfacedToNetworkConsumer() throws Exception {
        Reader reader = new Reader(0, mock(PartitionRequestQueue.class));
        try {
            IOException failure = new IOException("native drain failed");
            Field field = OmniCreditBasedSequenceNumberingViewReader.class.getDeclaredField("nativeReadFailure");
            field.setAccessible(true);
            field.set(reader, failure);
            assertTrue(reader.getAvailabilityAndBacklog().isAvailable());
            assertSame(failure, assertThrows(IOException.class, reader::getNextBuffer));
        } finally { reader.stop(); }
    }

    @Test
    void notificationsReuseFlinkQueueAndStopIsIdempotent() throws Exception {
        PartitionRequestQueue queue = mock(PartitionRequestQueue.class);
        Reader reader = new Reader(1, queue);
        reader.notifyNativeDataAvailable();
        verify(queue).notifyReaderNonEmpty(reader);
        reader.stop();
        reader.stop();
        reader.notifyNativeDataAvailable();
        verifyNoMoreInteractions(queue);
        assertEquals(1, reader.stops);
        assertEquals(1, reader.releases);
        assertFalse(reader.getAvailabilityAndBacklog().isAvailable());
    }
}
