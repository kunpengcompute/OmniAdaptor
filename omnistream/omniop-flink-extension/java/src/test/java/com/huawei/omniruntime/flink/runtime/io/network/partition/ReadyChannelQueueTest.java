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

package com.huawei.omniruntime.flink.runtime.io.network.partition;

import org.junit.jupiter.api.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class ReadyChannelQueueTest {
    @Test
    void coalescesNotificationsAndRotatesChannels() throws Exception {
        ReadyChannelQueue<String> ready = new ReadyChannelQueue<>();
        ready.add("a");
        ready.add("a");
        ready.add("b");
        assertEquals("a", ready.take());
        ready.add("a");
        assertEquals("b", ready.take());
        assertEquals("a", ready.take());
        assertNull(ready.poll());
    }

    @Test
    void arrivalAfterDequeueIsNotLost() throws Exception {
        ReadyChannelQueue<Integer> ready = new ReadyChannelQueue<>();
        ready.add(1);
        assertEquals(1, ready.take());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> ready.add(1)).get(5, TimeUnit.SECONDS);
            assertEquals(1, ready.take());
            assertNull(ready.poll());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void closeWakesIdleConsumerAndRejectsLateNotifications() throws Exception {
        ReadyChannelQueue<Integer> ready = new ReadyChannelQueue<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        try {
            Future<Integer> waiting = executor.submit(() -> {
                entered.countDown();
                return ready.take();
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            ready.close();
            assertNull(waiting.get(5, TimeUnit.SECONDS));
            ready.add(1);
            assertNull(ready.poll());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void concurrentArrivalsDuringDrainDoNotStrandData() throws Exception {
        ReadyChannelQueue<Integer> ready = new ReadyChannelQueue<>();
        AtomicInteger produced = new AtomicInteger();
        AtomicInteger consumed = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        final int count = 20000;
        try {
            Future<?> consumer = executor.submit(() -> {
                try {
                    while (consumed.get() < count) {
                        assertNotNull(ready.take());
                        int available = produced.get();
                        consumed.set(available);
                        if (produced.get() > available) {
                            ready.add(1);
                        }
                    }
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            });
            Future<?> producer = executor.submit(() -> {
                for (int i = 0; i < count; ++i) {
                    produced.incrementAndGet();
                    ready.add(1);
                }
            });
            producer.get(10, TimeUnit.SECONDS);
            consumer.get(10, TimeUnit.SECONDS);
            assertEquals(count, consumed.get());
        } finally {
            ready.close();
            executor.shutdownNow();
        }
    }
}
