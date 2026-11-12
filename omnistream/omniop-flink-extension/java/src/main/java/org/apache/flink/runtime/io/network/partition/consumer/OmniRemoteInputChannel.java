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

package org.apache.flink.runtime.io.network.partition.consumer;


import java.io.IOException;
import java.util.Optional;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

/** Reuses Flink's receive queue, sequence checks and credit management. */
public class OmniRemoteInputChannel extends RemoteInputChannel {
    private volatile Runnable dataAvailableListener;
    private boolean connected;

    public OmniRemoteInputChannel(
            SingleInputGate inputGate, int channelIndex, ResultPartitionID partitionId,
            int consumedSubpartitionIndex, ConnectionID connectionId,
            ConnectionManager connectionManager, int initialBackoff, int maxBackoff,
            int networkBuffersPerChannel, Counter numBytesIn, Counter numBuffersIn,
            ChannelStateWriter stateWriter) {
        super(inputGate, channelIndex, partitionId, consumedSubpartitionIndex,
                connectionId, connectionManager, initialBackoff, maxBackoff,
                networkBuffersPerChannel, numBytesIn, numBuffersIn, stateWriter);
    }

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    public int getGateIndex() {
        return getChannelInfo().getGateIdx();
    }

    // Widen visibility for the native fetcher, which lives outside this package.
    @Override
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer() throws IOException {
        // The native fetcher consumes this channel directly, bypassing InputGate.checkError().
        checkError();
        return super.getNextBuffer();
    }

    @Override
    public void releaseAllResources() throws IOException {
        super.releaseAllResources();
    }

    public void setDataAvailableListener(Runnable listener) {
        dataAvailableListener = listener;
        // Also probe once for data or an asynchronous error arriving before registration.
        if (listener != null) {
            listener.run();
        }
    }

    @Override
    protected void notifyChannelNonEmpty() {
        Runnable listener = dataAvailableListener;
        if (listener == null) {
            super.notifyChannelNonEmpty();
        } else {
            listener.run();
        }
    }

    @Override
    public void notifyPriorityEvent(int sequenceNumber) {
        Runnable listener = dataAvailableListener;
        if (listener == null) {
            super.notifyPriorityEvent(sequenceNumber);
        } else {
            listener.run();
        }
    }
}
