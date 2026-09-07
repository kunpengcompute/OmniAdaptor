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

import com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor.ResultPartitionIDPOJO;
import com.huawei.omniruntime.flink.runtime.io.network.buffer.PendingRecycleBuffer;
import com.huawei.omniruntime.flink.runtime.io.network.buffer.EventBuffer;
import com.huawei.omniruntime.flink.runtime.metrics.exception.GeneralRuntimeException;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.ReadOnlySlicedNetworkBuffer;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Queue;
import java.util.Timer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkState;

public class OmniLocalInputChannel extends LocalInputChannel {
    private static final Logger LOG = LoggerFactory.getLogger(OmniLocalInputChannel.class);
    private static final Field REQUEST_LOCK_FIELD;
    private static final Field SUBPARTITION_VIEW_FIELD;

    static {
        try {
            REQUEST_LOCK_FIELD = LocalInputChannel.class.getDeclaredField("requestLock");
            REQUEST_LOCK_FIELD.setAccessible(true);
            SUBPARTITION_VIEW_FIELD = LocalInputChannel.class.getDeclaredField("subpartitionView");
            SUBPARTITION_VIEW_FIELD.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private LocalInputChannel localInputChannel;
    private long nativeTaskRef;
    private ResultPartitionManager partitionManager;
    private long nativeLocalInputChannelRef;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private volatile boolean running = true;
    private Queue<PendingRecycleBuffer> pendingRecycleBuffers = new ConcurrentLinkedQueue<>();
    private AtomicInteger capacity = new AtomicInteger(100);
    private String taskName;
    /** Issue #81: retrigger must run on this wrapper, not the gate's orphan LocalInputChannel. */
    private Timer localRetriggerTimer;

    public OmniLocalInputChannel(LocalInputChannel localInputChannel, ResultPartitionManager partitionManager,
            long nativeTaskRef, SingleInputGate singleInputGate, int initialBackoff, int maxBackoff,
            Counter numBytesIn, Counter numBuffersIn, TaskEventPublisher taskEventPublisher,
            ChannelStateWriter channelStateWriter, String taskName) {
        super(singleInputGate, localInputChannel.getChannelIndex(), localInputChannel.getPartitionId(),
                localInputChannel.consumedSubpartitionIndex, partitionManager, taskEventPublisher, initialBackoff,
                maxBackoff, numBytesIn, numBuffersIn, channelStateWriter);
        this.partitionManager = partitionManager;
        this.localInputChannel = localInputChannel;
        this.nativeTaskRef = nativeTaskRef;
        this.taskName = taskName;
    }

    private Object requestLock() {
        try {
            return REQUEST_LOCK_FIELD.get(this);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    private void setSubpartitionView(ResultSubpartitionView view) {
        try {
            SUBPARTITION_VIEW_FIELD.set(this, view);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    private void scheduleSelfRetrigger() {
        synchronized (requestLock()) {
            if (localRetriggerTimer == null) {
                localRetriggerTimer = new Timer(true);
            }
            retriggerSubpartitionRequest(localRetriggerTimer);
        }
    }

    /**
     * Same bind logic as {@link LocalInputChannel#requestSubpartition()}, but on PartitionNotFound
     * schedules retry on <b>this</b> Omni wrapper instead of {@code inputGate.retriggerPartitionRequest}
     * (which looks up the unrelated LocalInputChannel still stored in the gate map).
     */
    @Override
    protected void requestSubpartition() throws IOException {
        boolean retriggerRequest = false;
        boolean notifyDataAvailable = false;

        synchronized (requestLock()) {
            checkState(!isReleased(), "LocalInputChannel has been released already");

            if (getSubpartitionView() == null) {
                LOG.debug(
                        "{}: Requesting LOCAL subpartition {} of partition {} (OmniLocalInputChannel).",
                        this,
                        consumedSubpartitionIndex,
                        partitionId);

                try {
                    ResultSubpartitionView view =
                            partitionManager.createSubpartitionView(
                                    partitionId, consumedSubpartitionIndex, this);

                    if (view == null) {
                        throw new IOException("Error requesting subpartition.");
                    }

                    setSubpartitionView(view);

                    if (isReleased()) {
                        view.releaseAllResources();
                        setSubpartitionView(null);
                    } else {
                        notifyDataAvailable = true;
                    }
                } catch (PartitionNotFoundException notFound) {
                    if (increaseBackoff()) {
                        retriggerRequest = true;
                    } else {
                        throw notFound;
                    }
                }
            }
        }

        if (notifyDataAvailable) {
            notifyDataAvailable();
        }

        if (retriggerRequest) {
            scheduleSelfRetrigger();
        }
    }

    public boolean changeNativeLocalInputChannel() {
        ResultPartitionIDPOJO resultPartitionIDPOJO = new ResultPartitionIDPOJO(localInputChannel.getPartitionId());
        JSONObject jsonObject = new JSONObject(resultPartitionIDPOJO);
        String parititonIdString = jsonObject.toString();
        nativeLocalInputChannelRef = doChangeNativeLocalInputChannel(nativeTaskRef, parititonIdString);
        if(nativeLocalInputChannelRef != -1) {
            registerJavaOmniLocalInputChannel(nativeLocalInputChannelRef);
            LOG.info("Successfully changed native local input channel for OmniLocalInputChannel, taskName: {}, channelInfo: {}, nativeLocalInputChannelRef = {}",
                    taskName, localInputChannel.getChannelInfo(), nativeLocalInputChannelRef);
            return true;
        } else {
            LOG.error("Failed to change native local input channel for OmniLocalInputChannel, taskName: {}, channelInfo: {}", taskName, localInputChannel.getChannelInfo());
            return false;
        }
    }

    public void doRequestSubpartition() throws IOException {
        LOG.info("Requesting subpartition for OmniLocalInputChannel, taskName: {}, channelInfo: {}", taskName, localInputChannel.getChannelInfo());
        requestSubpartition();
    }
    
    @Override
    public synchronized void notifyDataAvailable() {
        doGetNextBuffer();
    }
    
    public void doGetNextBuffer() {
        try {
            checkCapacityAvailability();
            Optional<BufferAndAvailability> bufferAndAvailability = getNextBuffer();
            if (bufferAndAvailability.isPresent()) {
                // Notify the native layer that data is available
                BufferAndAvailability ba = bufferAndAvailability.get();
                ReadOnlySlicedNetworkBuffer readOnlySlicedNetworkBuffer = (ReadOnlySlicedNetworkBuffer) ba.buffer();
                int bufferType = ba.buffer().isBuffer() ? 0 : 1; // 0 for Buffer, 1 for Event, underline only
                if (bufferType == 1 && readOnlySlicedNetworkBuffer.getDataType().isBlockingUpstream()) {
                    bufferType++;
                    if (readOnlySlicedNetworkBuffer.getDataType().toString() == "ALIGNED_CHECKPOINT_BARRIER") {
                        bufferType++;;
                    }
                }
                MemorySegment memorySegment = readOnlySlicedNetworkBuffer.getMemorySegment();
                int readIndex = readOnlySlicedNetworkBuffer.readerIndex();
                int length = readOnlySlicedNetworkBuffer.readableBytes();
                int memorySegmentOffset = readOnlySlicedNetworkBuffer.getMemorySegmentOffset();
                int sequenceNumber = ba.getSequenceNumber();
                long segmentAddress;
                if (bufferType != 0) {
                    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(length);
                    byte[] heapArr = memorySegment.getArray();
                    if (heapArr.length >= 3 && heapArr[3] == 8) {
                        getSubpartitionView().acknowledgeAllDataProcessed();
                    }
                    byteBuffer.put(heapArr, memorySegmentOffset, length);
                    memorySegment = MemorySegmentFactory.wrapOffHeapMemory(byteBuffer);
                    segmentAddress = memorySegment.getAddress();
                    memorySegmentOffset = 0;
                    readIndex = 0;
                    EventBuffer eventBuffer = new EventBuffer(memorySegment);
                    fillBufferRecycler(segmentAddress, eventBuffer);
                    LOG.info("Received event buffer with address: {}, taskName: {}, channelInfo: {}", segmentAddress, taskName, localInputChannel.getChannelInfo());
                } else {
                    segmentAddress = memorySegment.getAddress();
                    fillBufferRecycler(segmentAddress, readOnlySlicedNetworkBuffer);
                    capacity.decrementAndGet();
                }
                // support buffer and event so far
                // Call the native method to send data
                LOG.debug("***************** Sending buffer to native layer for OmniLocalInputChannel with " +
                                "channelIndex: {} of task: {}, address = {}, readIndex = {}, length = {}, " +
                                "memorySegmentOffset = {}, sequenceNumber = {}, bufferType = {}, capacity = {}",
                        localInputChannel.getChannelIndex(), taskName, segmentAddress, readIndex, length,
                        memorySegmentOffset, sequenceNumber, bufferType, capacity.get());
                sendMemorySegmentToNative(nativeLocalInputChannelRef, segmentAddress, readIndex, length, memorySegmentOffset, sequenceNumber, bufferType);
                if (ba.moreAvailable()) {
                    doGetNextBuffer();
                }
            } else {
                LOG.info("No more data available for OmniLocalInputChannel, taskName: {}, channelInfo: {}", taskName, localInputChannel.getChannelInfo());
            }
        } catch (IOException e) {
            throw new GeneralRuntimeException(e);
        }
    }
    
    private void checkCapacityAvailability() {
        synchronized (this) {
            if (capacity.get() <= 0) {
                try {
                    LOG.warn("Capacity is zero, wait for it to be recycled.");
                    wait(); // Adjust the timeout as needed
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.error("Thread interrupted while waiting for buffer to be recycled", e);
                }
            }
        }
    }
    
    private void fillBufferRecycler(long address, Buffer buffer) {
        if (address != -1 && buffer != null) {
            PendingRecycleBuffer prb = new PendingRecycleBuffer(buffer, address);
            pendingRecycleBuffers.add(prb);
        }
    }

    @Override
    protected void notifyChannelNonEmpty() {
        LOG.info("notifyChannelNonEmpty OmniLocalInputChannel, taskName: {}, channelInfo: {}", taskName, localInputChannel.getChannelInfo());
        notifyDataAvailable();
    }

    @Override
    public void releaseAllResources() throws IOException {
        running = false;
        executorService.shutdownNow();
        super.releaseAllResources();
        LOG.info("Releasing all resources for OmniLocalInputChannel, taskName: {}, channelInfo: {}", taskName, localInputChannel.getChannelInfo());
    }
    
    public void startRecycleBuffersThread() {
        executorService.execute(new BufferRecycleThread());
    }
    
    class BufferRecycleThread implements Runnable {
        @Override
        public void run() {
            try {
                while (running) {
                    long address = getRecycleBufferAddress(nativeLocalInputChannelRef);
                    if (address != -1) {
                        PendingRecycleBuffer pendingRecycleBuffer = pendingRecycleBuffers.peek();
                        if (pendingRecycleBuffer != null) {
                            if (address == pendingRecycleBuffer.getAddress()) {
                                pendingRecycleBuffers.poll();
                                doRecycleBuffers(pendingRecycleBuffer);
                            } else {
                                LOG.warn("Received address {} does not match the pending recycle buffer address {}. "
                                                + "This may indicate a mismatch in buffer recycling.", address,
                                        pendingRecycleBuffer.getAddress());
                            }
                        } else {
                            if (address == -9999) {
                                LOG.info("Received special address -9999, indicating no buffer to recycle.");
                                break;
                            }
                            LOG.warn("No buffer found for address: {}", address);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Error in recycle buffers thread", e);
                executorService.shutdown();
            } finally {
                pendingRecycleBuffers.forEach(buffer -> {
                    try {
                        doRecycleBuffers(buffer);
                    } catch (Exception ex) {
                        LOG.error("Error recycling buffer", ex);
                    }
                });
                pendingRecycleBuffers.clear();
                LOG.info("****************************OmniLocalInputChannel Recycling buffer thread finished for " + "taskName = {}, channelInfo: {}", taskName, localInputChannel.getChannelInfo());
            }
        }
    }
    
    private void doRecycleBuffers(PendingRecycleBuffer pendingRecycleBuffer) {
        pendingRecycleBuffer.getBuffer().recycleBuffer();
        if (!(pendingRecycleBuffer.getBuffer() instanceof EventBuffer)) {
            synchronized (this) {
                capacity.incrementAndGet();
                LOG.debug("%%%%%%%%%%%%% Recycling buffer with address: {} and capacity = {} ",
                        pendingRecycleBuffer.getAddress(), capacity);
                notifyAll(); // Notify waiting threads that a buffer has been recycled
            }
        }
    }

    public void doResumeConsumption() {
        resumeConsumption();
    }
    
    
    public native long doChangeNativeLocalInputChannel(long nativeTaskRef, String partitionId);
    public native void sendMemorySegmentToNative(long omniLocalInputChannelRef, long segmentAddress, int readIndex,
            int length, int memorySegmentOffset, int sequenceNumber, int bufferType);
    public native long getRecycleBufferAddress(long omniLocalInputChannelRef);
    private native void registerJavaOmniLocalInputChannel(long omniLocalInputChannelRef);
    
    
}
