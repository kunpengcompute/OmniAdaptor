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

package org.apache.flink.runtime.io.network.netty;

import static org.apache.flink.runtime.io.network.api.StopMode.DRAIN;
import static com.huawei.omniruntime.flink.core.memory.MemoryUtils.getByteBufferAddress;

import com.huawei.omniruntime.flink.core.memory.MemoryUtils;
import com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor.ResultPartitionIDPOJO;
import com.huawei.omniruntime.flink.runtime.io.network.buffer.NativeBufferRecycler;
import com.huawei.omniruntime.flink.runtime.io.network.buffer.NativeNetworkBufferRecycler;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.*;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * OmniCreditBasedSequenceNumberingViewReader
 *
 * @since 2025-04-27
 */
public class OmniCreditBasedSequenceNumberingViewReader
        extends CreditBasedSequenceNumberingViewReader implements Runnable {
    private static final EndOfData END_OF_DATA = new EndOfData(DRAIN);
    private static final Logger LOG = LoggerFactory.getLogger(OmniCreditBasedSequenceNumberingViewReader.class);

    private long nativeTaskRef = -1;
    private volatile long nativeCreditBasedSequenceNumberingViewReaderRef = -1;
    private String taskName;
    private final Object requestLock = new Object();

    private ByteBuffer outputBuffer;
    private long outputBufferAddress;   // the address of outputBuffer
    private int outputBufferCapacity;
    private long statusAddress;

    private volatile boolean running = true;

    AtomicInteger sequenceNumber = new AtomicInteger(0);

    // for JNI to return value, should be changed to directbytebuff later, avoid pass object/array through JNI

    private ByteBuffer outputBufferStatus;
    private List<BufferInfo> bufferInfos = new ArrayList<>();

    private int subPartitionIndex;
    private String partitionId;
    private volatile boolean flushed = false;
    private volatile int numCreditsAvailable;
    private volatile int initialCredit;
    // ExecutorService, not Executor: this must be shut down in stop(), otherwise its worker thread
    // parks on the task queue forever and one thread leaks per view reader per job.
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Object localChannelLocker = new Object();

    OmniCreditBasedSequenceNumberingViewReader(
            InputChannelID receiverId, int initialCredit, PartitionRequestQueue requestQueue) {
        super(receiverId, initialCredit, requestQueue);
        this.initialCredit = initialCredit;
        numCreditsAvailable = initialCredit;
        initDirectBuffers();
    }

    /**
     * initDirectBuffers
     */
    public void initDirectBuffers() {
        // the init outputBuffer size  with an initial size of 128
        // [ elementNum(default 10) * (memorySegmentAddress+elementAddress + elementLength+memoryType[vectorBatch:0,memorySegment:1,event:2])  10 * (8+8+4+4) = 240 powerOfTwo(240) = 256 ]
        // in the future maybe we can change the size of outputBuffer dynamically,
        // but  for the vectorBatch data type scenario,
        // the fixed size of outputBuffer  is not a problem
        this.outputBufferCapacity = 256;
        this.outputBuffer = ByteBuffer.allocateDirect(this.outputBufferCapacity);
        this.outputBufferAddress = getByteBufferAddress(outputBuffer);

        ByteOrder nativeOrder = ByteOrder.nativeOrder();

        if (nativeOrder == ByteOrder.BIG_ENDIAN) {
            System.out.println("Native byte order is big-endian");
        } else if (nativeOrder == ByteOrder.LITTLE_ENDIAN) {
            System.out.println("Native byte order is little-endian");
        } else {
            System.out.println("Unknown byte order");
        }

        // long address 8, int capacity 4 ,
        // int resultLength (length in bytes) 4 ,
        // int num of element 4, int output buffer owner 4 (owner =0 java 1 cpp native
        this.outputBufferStatus = ByteBuffer.allocateDirect(32);
        this.outputBufferStatus.order(ByteOrder.nativeOrder());
        this.statusAddress = getByteBufferAddress(outputBufferStatus);
        outputBufferStatus.putLong(outputBufferAddress);
        outputBufferStatus.putInt(this.outputBufferCapacity);
        outputBufferStatus.position(20); // owner
        outputBufferStatus.putInt(0);  // output buffer is owned by java
    }


    @Override
    public void requestSubpartitionView(
            ResultPartitionProvider partitionProvider,
            ResultPartitionID resultPartitionId,
            int subPartitionIndex)
            throws IOException {
        synchronized (requestLock) {
            ResultPartitionIDPOJO resultPartitionIDPOJO = new ResultPartitionIDPOJO(resultPartitionId);
            JSONObject jsonObject = new JSONObject(resultPartitionIDPOJO);
            this.subPartitionIndex = subPartitionIndex;
            this.partitionId = jsonObject.toString();

            BufferWritingResultPartition producer =
                    resolveProducerPartition(partitionProvider, resultPartitionId);
            setNativeTaskRef(producer.getNativeTaskRef());
            setTaskName(producer.getOwningTaskName());

            // Step 1 — Native bind first: Java readView is not occupied yet on failure.
            LOG.info("requestSubpartitionView for task: {} ## {}", taskName.substring(0, 15), subPartitionIndex);
            nativeCreditBasedSequenceNumberingViewReaderRef = createNativeCreditBasedSequenceNumberingViewReader(
                    nativeTaskRef, statusAddress, partitionId, subPartitionIndex);
            if (nativeCreditBasedSequenceNumberingViewReaderRef == -1) {
                LOG.error("create nativeCreditBasedSequenceNumberingViewReader failed");
                throw new PartitionNotFoundException(resultPartitionId);
            }
            LOG.info("requestSubpartitionView for task: {} ## {} create result = {},{}",
                    taskName.substring(0, 15),
                    subPartitionIndex, nativeCreditBasedSequenceNumberingViewReaderRef,
                    this.hashCode());

            // Step 2 — Java bind via super: occupies subpartition.readView only after Native succeeds.
            try {
                super.requestSubpartitionView(partitionProvider, resultPartitionId, subPartitionIndex);
            } catch (Exception e) {
                LOG.error("Java bind failed after Native bind succeeded, cleaning up native reader", e);
                stop();
                if (e instanceof IOException) {
                    throw (IOException) e;
                }
                throw new PartitionNotFoundException(resultPartitionId);
            }

            Thread thread = new Thread(this);
            thread.start();
            startFirstDataAvailableNotificationThread();
        }
    }

    private static BufferWritingResultPartition resolveProducerPartition(
            ResultPartitionProvider partitionProvider, ResultPartitionID resultPartitionId)
            throws PartitionNotFoundException {
        if (!(partitionProvider instanceof ResultPartitionManager)) {
            throw new PartitionNotFoundException(resultPartitionId);
        }
        ResultPartitionManager resultPartitionManager = (ResultPartitionManager) partitionProvider;
        try {
            Field field = ResultPartitionManager.class.getDeclaredField("registeredPartitions");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<ResultPartitionID, ResultPartition> registeredPartitions =
                    (Map<ResultPartitionID, ResultPartition>) field.get(resultPartitionManager);
            ResultPartition resultPartition = registeredPartitions.get(resultPartitionId);
            if (resultPartition == null) {
                throw new PartitionNotFoundException(resultPartitionId);
            }
            if (!(resultPartition instanceof BufferWritingResultPartition)) {
                throw new PartitionNotFoundException(resultPartitionId);
            }
            BufferWritingResultPartition producer = (BufferWritingResultPartition) resultPartition;
            if (!producer.isNative()) {
                throw new PartitionNotFoundException(resultPartitionId);
            }
            return producer;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to resolve producer partition", e);
        }
    }

    private void startFirstDataAvailableNotificationThread() {
        executor.execute(() -> {
            synchronized (localChannelLocker) {
                if (nativeCreditBasedSequenceNumberingViewReaderRef == -1) {
                    try {
                        localChannelLocker.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                // here, nativeCreditBasedSequenceNumberingViewReaderRef should not be -1
                firstDataAvailableNotification(nativeCreditBasedSequenceNumberingViewReaderRef);
                LOG.info("call first notification  for task: {} ## {} successfully..........", taskName.substring(0,
                        15), subPartitionIndex);
            }
            
        });
    }


    @Override
    public void run() {
        Thread.currentThread().setName("OmniCreditBasedSequenceNumberingViewReader::::["
                                        + taskName.substring(0, 15) + "##]" + subPartitionIndex);
        int count = 0;
        while (running) {
            try {
                if (nativeCreditBasedSequenceNumberingViewReaderRef != -1) {
                    if (!flushed && (numCreditsAvailable - bufferInfos.size()) > 0) {
                        int res = checkIfDataAvailableAndNotifyNetty();
                        LOG.debug("nativeCreditBasedSequenceNumberingViewReaderRef can flush: " + "{} ## {},"
                                + " got " + "data" + " size = {} and numCreditsAvailable = {},native ref = "
                                + "{}", taskName.substring(0, 15), subPartitionIndex, res,
                                numCreditsAvailable, nativeCreditBasedSequenceNumberingViewReaderRef);
                        if (res == 0) {
                            Thread.sleep(20);
                        }
                    } else {
                        count++;
                        if (count % 500 == 0) {
                            LOG.debug("nativeCreditBasedSequenceNumberingViewReaderRef can not flush: {} "
                                    + "##{}, numCreditsAvailable = {}, bufferInfos size = {} and "
                                    + "flushed= {} and native " + "ref = {}", taskName.substring(0,
                                    15), subPartitionIndex, numCreditsAvailable, bufferInfos.size(),
                                    flushed, nativeCreditBasedSequenceNumberingViewReaderRef);
                            count = 0;
                        }
                        Thread.sleep(20);
                    }
                } else {
                    synchronized (localChannelLocker) {
                        LOG.error("nativeCreditBasedSequenceNumberingViewReaderRef is -1 for task: {} ## {},{},{}",
                                taskName.substring(0, 15), subPartitionIndex, this.hashCode(),
                                this.nativeCreditBasedSequenceNumberingViewReaderRef);
                        nativeCreditBasedSequenceNumberingViewReaderRef =
                                createNativeCreditBasedSequenceNumberingViewReader(nativeTaskRef, statusAddress,
                                        partitionId, subPartitionIndex);
                        if (nativeCreditBasedSequenceNumberingViewReaderRef != -1) {
                            localChannelLocker.notifyAll();
                        }
                    }

                }
            } catch (InterruptedException e) {
                LOG.error("InterruptedException in OmniCreditBasedSequenceNumberingViewReader", e);
            }
        }
    }

    private synchronized int checkIfDataAvailableAndNotifyNetty() {
        if (nativeCreditBasedSequenceNumberingViewReaderRef == -1) {
            return 0;
        }
        int res = getAvailabilityAndBacklog(nativeCreditBasedSequenceNumberingViewReaderRef, getNumCreditsAvailable());
        if (res > 0) {
            flushed = true;
            notifyDataAvailableForNetty();
        }
        return res;
    }

    /**
     * notifyDataAvailable
     */
    @Override
    public void notifyDataAvailable() {
        LOG.info("Thread = {} do nothing notifyDataAvailable for task: {} ## {} and native red = {}",
                Thread.currentThread().getName(), taskName.substring(0, 15),
                subPartitionIndex, nativeCreditBasedSequenceNumberingViewReaderRef);
    }

    /**
     * notifyDataAvailableForNetty
     */
    public void notifyDataAvailableForNetty() {
        LOG.info("Thread = {} do super notifyDataAvailable for task: {} ## {} and native ref = {}",
                Thread.currentThread().getName(), taskName.substring(0, 15),
                subPartitionIndex, nativeCreditBasedSequenceNumberingViewReaderRef);

        super.notifyDataAvailable();
    }

    public void setNativeTaskRef(long nativeTaskRef) {
        this.nativeTaskRef = nativeTaskRef;
    }

    public long getNativeTaskRef() {
        return nativeTaskRef;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    /**
     * getAvailabilityAndBacklog
     *
     * @return ResultSubpartitionView.AvailabilityWithBacklog
     */
    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog() {
        if (nativeCreditBasedSequenceNumberingViewReaderRef == -1) {
            return new ResultSubpartitionView.AvailabilityWithBacklog(false, 0);
        }
        int backlog = this.bufferInfos.size();

        boolean isAvailable = false;
        if (getNumCreditsAvailable() > 0) {
            isAvailable = true;
        } else {
            LOG.info("OmniCreditBasedSequenceNumberingViewReader IS not available, backlog = {} and "
                    + "available = {} of {}## {} and native ref = {}, thread = {}", backlog, isAvailable,
                    taskName.substring(0, 15), subPartitionIndex,
                    nativeCreditBasedSequenceNumberingViewReaderRef, Thread.currentThread().getName());
        }
        return new ResultSubpartitionView.AvailabilityWithBacklog(isAvailable, backlog);
    }

    static class BufferInfo {
        private NetworkBuffer networkBuffer;
        private long address;
        private int length;

        public BufferInfo(NetworkBuffer networkBuffer, long address, int length) {
            this.networkBuffer = networkBuffer;
            this.address = address;
            this.length = length;
        }

        public long getAddress() {
            return address;
        }

        public int getLength() {
            return length;
        }

        public NetworkBuffer getNetworkBuffer() {
            return networkBuffer;
        }
    }


    @Override
    public InputChannel.BufferAndAvailability getNextBuffer() throws IOException {
        if (numCreditsAvailable > 0) {
            InputChannel.BufferAndAvailability bufferAndAvailability = doGetNextBuffer();

            if (bufferAndAvailability != null) {
                return bufferAndAvailability;
            } else {
                // get next buffer from c++
                // The native code will put the data in the serializedBatchQueue to the heap-off memory.
                // The java code can directly decode the data from the heap-off memory,
                // and save to the bufferInfos.
                // "readElementNum" is the size of serializedBatchQueue in the native code (the max value is 10)
                int readElementNum = getNextBuffer(nativeCreditBasedSequenceNumberingViewReaderRef);
                if (readElementNum > 0) {
                    // decode the buffer
                    decodeReadBuffer(readElementNum);
                    return doGetNextBuffer();
                } else {
                    return null;
                }
            }
        } else {
            return null;
        }
    }

    private InputChannel.BufferAndAvailability doGetNextBuffer() throws IOException {
        if (!bufferInfos.isEmpty()) {
            flushed = false;
            BufferInfo bufferInfo = bufferInfos.get(0);
            if (bufferInfo.networkBuffer.isBuffer()) {
                if(numCreditsAvailable == 0){
                    throw new IllegalStateException("no credit available");
                }else {
                    numCreditsAvailable--;
                }
            }
            //remove the element from bufferInfos
            bufferInfos.remove(0);
            Buffer.DataType nextDataType = bufferInfos.size() > 0 ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.NONE;

            return new InputChannel.BufferAndAvailability(
                    bufferInfo.getNetworkBuffer(),
                    nextDataType,
                    bufferInfos.size(),
                    sequenceNumber.getAndIncrement());
        } else {
            return null;
        }
    }

    private void decodeReadBuffer(int readElementNum) {
        outputBuffer.position(0);
        outputBuffer.order(ByteOrder.LITTLE_ENDIAN);
        // decode the buffer
        for (int i = 0; i < readElementNum; i++) {
            long  nativeMemorySegmentAddress = outputBuffer.getLong();
            long dataAddress = outputBuffer.getLong();
            int length = outputBuffer.getInt();
            int memoryType = outputBuffer.getInt();
            // 0 for vectorBatch, 1 for memorySegment 2 for event data
            if (memoryType == 0 || memoryType == 1 || memoryType == 2) {
                doDecodeVectorBatchBufferOrMemorySegmentBuffer(nativeMemorySegmentAddress,dataAddress, length,memoryType);
            } else {
                LOG.error("Unknown memory type: {} for task: {} ## {}", memoryType,
                        taskName.substring(0, 15), subPartitionIndex);
            }
        }
        // reset the outputBuffer
        outputBuffer.clear();
    }


    private void doDecodeVectorBatchBufferOrMemorySegmentBuffer(long nativeMemorySegmentAddress,long dataAddress, int length,int memoryType) {
        ByteBuffer resultBuffer = MemoryUtils.wrapUnsafeMemoryWithByteBuffer(dataAddress, length);
        resultBuffer.order(ByteOrder.BIG_ENDIAN);
        MemorySegment memorySegment = MemorySegmentFactory.wrapOffHeapMemory(resultBuffer);

        NativeBufferRecycler nativeBufferRecycler  = null;
        if(memoryType == 0) {
            nativeBufferRecycler = NativeBufferRecycler.getInstance(nativeCreditBasedSequenceNumberingViewReaderRef);
        }else {
            nativeBufferRecycler = NativeNetworkBufferRecycler.getInstance(nativeCreditBasedSequenceNumberingViewReaderRef);
        }
        nativeBufferRecycler.registerMemorySegment(memorySegment, nativeMemorySegmentAddress);

        NetworkBuffer buffer = new NetworkBuffer(memorySegment, nativeBufferRecycler);
        if(memoryType == 2) {
            buffer.setDataType(Buffer.DataType.EVENT_BUFFER);
        }
        buffer.setReaderIndex(0);
        buffer.setSize(length);
        bufferInfos.add(new BufferInfo(buffer, dataAddress, length));
    }

//    private void doDecodeVectorBatchBuffer(long nativeMemorySegmentAddress,long dataAddress, int length) {
//            ByteBuffer resultBuffer = MemoryUtils.wrapUnsafeMemoryWithByteBuffer(dataAddress, length);
//            resultBuffer.order(ByteOrder.BIG_ENDIAN);
//            MemorySegment memorySegment = MemorySegmentFactory.wrapOffHeapMemory(resultBuffer);
//            NativeBufferRecycler nativeBufferRecycler =
//                    NativeBufferRecycler.getInstance(nativeCreditBasedSequenceNumberingViewReaderRef);
//            nativeBufferRecycler.registerMemorySegment(memorySegment, nativeMemorySegmentAddress);
//
//            NetworkBuffer buffer = new NetworkBuffer(memorySegment, nativeBufferRecycler);
//
//            buffer.setReaderIndex(0);
//            buffer.setSize(length);
//            bufferInfos.add(new BufferInfo(buffer, dataAddress, length));
//    }
//
//    private void doDecodeMemorySegmentBuffer(long address, int length,int memoryType) {
//        MemorySegment memorySegment = MemorySegmentFactory.wrapOffHeapMemory(
//                MemoryUtils.wrapUnsafeMemoryWithByteBuffer(address, length));
//        NativeBufferRecycler nativeBufferRecycler =
//                NativeNetworkBufferRecycler.getInstance(nativeCreditBasedSequenceNumberingViewReaderRef);
//        nativeBufferRecycler.registerMemorySegment(memorySegment, address);
//
//        NetworkBuffer buffer = new NetworkBuffer(memorySegment, nativeBufferRecycler);
//        buffer.order(ByteOrder.BIG_ENDIAN);
//        if(memoryType == 2) {
//            buffer.setDataType(Buffer.DataType.EVENT_BUFFER);
//        }
//        buffer.setReaderIndex(0);
//        buffer.setSize(length);
//        bufferInfos.add(new BufferInfo(buffer, address, length));
//    }

    private AbstractEvent getEventById(int type) {
        if (type == 0) {
            return EndOfPartitionEvent.INSTANCE;
        } else if (type == 1) {
            return null;
        } else if (type == 2) {
            return EndOfSuperstepEvent.INSTANCE;
        } else if (type == 5) {
            return EndOfChannelStateEvent.INSTANCE;
        } else if (type == 8) {
            return END_OF_DATA;
        } else {
            return null;
        }
    }

    /**
     * stop
     */
    public void stop() {
        running = false;
        int res = checkIfDataAvailableAndNotifyNetty();
        int retryCount = 0;
        while (res > 0 && retryCount++ < 5) {
            LOG.info("OmniCreditBasedSequenceNumberingViewReader of {}## "
                            + "{}............................find data during stop......data size= "
                            + "{}................................",
                    taskName.substring(0, 15),
                    subPartitionIndex, res);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            res = checkIfDataAvailableAndNotifyNetty();
        }

        if (res > 0) {
            LOG.warn("There are still {} data available after stop, taskName: {}, partitionId: {}, subPartitionIndex: {}",
                    res, taskName.substring(0, 15), partitionId, subPartitionIndex);
        }

        NativeBufferRecycler.unRegisterInstance(nativeCreditBasedSequenceNumberingViewReaderRef);
        NativeNetworkBufferRecycler.unRegisterInstance(nativeCreditBasedSequenceNumberingViewReaderRef);

        LOG.info("OmniCreditBasedSequenceNumberingViewReader of {}## {} and native ref = {}"
                        + "............................is stopped......................................",
                taskName.substring(0, 15), subPartitionIndex, nativeCreditBasedSequenceNumberingViewReaderRef);
        // invoke native to call OmniCreditBasedSequenceNumberingViewReader::releaseAllResources()
        releaseNativeViewReader(nativeCreditBasedSequenceNumberingViewReaderRef);
        setNativeCreditBasedSequenceNumberingViewReaderRef(-1);

        // shutdownNow, not shutdown: the first-data-available task may still be blocked in
        // localChannelLocker.wait(), and only an interrupt releases it. Without this the worker
        // thread parks on the task queue forever - one leaked thread per view reader per job.
        synchronized (localChannelLocker) {
            localChannelLocker.notifyAll();
        }
        executor.shutdownNow();
    }

    private synchronized void setNativeCreditBasedSequenceNumberingViewReaderRef(long value) {
        nativeCreditBasedSequenceNumberingViewReaderRef = value;
    }


    /**
     * releaseAllResources
     *
     * @throws IOException IOException
     */
    public void releaseAllResources() throws IOException {
        super.releaseAllResources();
        LOG.info("--------------releaseAllResources for task: {} ## {} and native ref = {}",
                taskName.substring(0, 15), subPartitionIndex,
                nativeCreditBasedSequenceNumberingViewReaderRef);
        stop();
    }


    /**
     * resumeConsumption
     */
    @Override
    public void resumeConsumption() {
        if (initialCredit == 0) {
            // reset available credit if no exclusive buffer is available at the
            // consumer side for all floating buffers must have been released
            numCreditsAvailable = 0;
        }

        LOG.info("resumeConsumption and notify data available, taskName: {}, partitionId: {}, subPartitionIndex: {}",
                taskName.substring(0, 15), partitionId, subPartitionIndex);
        resumeConsumption(nativeCreditBasedSequenceNumberingViewReaderRef);

        // When subPartition is blocked, the flusher thread may not notify data available, so we need to notify it manually.
        startFirstDataAvailableNotificationThread();
    }

    /**
     * needAnnounceBacklog
     *
     * @return boolean
     */
    @Override
    public boolean needAnnounceBacklog() {
        return initialCredit == 0 && numCreditsAvailable == 0;
    }

    /**
     * addCredit
     *
     * @param creditDeltas creditDeltas
     */
    @Override
    public void addCredit(int creditDeltas) {
        LOG.info("got a credit from client for task: {} ## {}, credit = {} , addedCredit = {}",
                taskName.substring(0, 15),
                subPartitionIndex,
                numCreditsAvailable,
                creditDeltas);

        numCreditsAvailable += creditDeltas;
    }

    @Override
    int getNumCreditsAvailable() {
        return numCreditsAvailable;
    }

    /**
     * getAvailabilityAndBacklog
     *
     * @param nativeCreditBasedSequenceNumberingViewReaderRef nativeCreditBasedSequenceNumberingViewReaderRef
     * @param numCreditsAvailable numCreditsAvailable
     * @return int
     */
    public native int getAvailabilityAndBacklog(
            long nativeCreditBasedSequenceNumberingViewReaderRef,
            int numCreditsAvailable);

    /**
     * getNextBuffer
     *
     * @param nativeCreditBasedSequenceNumberingViewReaderRef nativeCreditBasedSequenceNumberingViewReaderRef
     * @return int
     */
    public native int getNextBuffer(long nativeCreditBasedSequenceNumberingViewReaderRef);

    // create native credit based sequence numbering view reader, this should be a native method
    /**
     * createNativeCreditBasedSequenceNumberingViewReader
     *
     * @param nativeTaskRef nativeTaskRef
     * @param statusAddress statusAddress
     * @param partitionId partitionId
     * @param subPartitionIndex subPartitionIndex
     * @return long
     */
    public native long createNativeCreditBasedSequenceNumberingViewReader(
            long nativeTaskRef,
            long statusAddress,
            String partitionId,
            int subPartitionIndex);

    /**
     * callFirstDataAvailableNotification
     *
     * @param nativeCreditBasedSequenceNumberingViewReaderRef nativeCreditBasedSequenceNumberingViewReaderRef
     */
    public native void firstDataAvailableNotification(long nativeCreditBasedSequenceNumberingViewReaderRef);

    private native void resumeConsumption(long nativeCreditBasedSequenceNumberingViewReaderRef);

    private native  void releaseNativeViewReader(long nativeCreditBasedSequenceNumberingViewReaderRef);
}