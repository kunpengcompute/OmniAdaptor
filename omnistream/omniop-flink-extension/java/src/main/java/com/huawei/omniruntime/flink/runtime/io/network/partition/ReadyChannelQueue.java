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

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

/** One queued entry per channel, removed before processing so arrivals cannot be lost. */
final class ReadyChannelQueue<T> {
    private final ArrayDeque<T> queue = new ArrayDeque<>();
    private final Set<T> queued = new HashSet<>();
    private boolean closed;

    synchronized void add(T channel) {
        if (!closed && queued.add(channel)) {
            queue.addLast(channel);
            notifyAll();
        }
    }

    synchronized T take() throws InterruptedException {
        while (queue.isEmpty() && !closed) {
            wait();
        }
        return poll();
    }

    synchronized T poll() {
        T channel = queue.pollFirst();
        if (channel != null) {
            queued.remove(channel);
        }
        return channel;
    }

    synchronized void close() {
        closed = true;
        notifyAll();
    }
}
