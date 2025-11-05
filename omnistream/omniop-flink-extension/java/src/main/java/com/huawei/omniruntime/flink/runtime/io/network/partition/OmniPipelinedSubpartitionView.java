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

package com.huawei.omniruntime.flink.runtime.io.network.partition;

import static com.huawei.omniruntime.flink.core.memory.MemoryUtils.getByteBufferAddress;

import com.huawei.omniruntime.flink.core.memory.MemoryUtils;
import com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor.ResultPartitionIDPOJO;
import com.huawei.omniruntime.flink.runtime.io.network.buffer.NativeBufferRecycler;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.ReadOnlySlicedNetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.PipelinedSubpartitionView;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OmniPipelinedSubpartitionView extends PipelinedSubpartitionView {
    private static final Logger LOG = LoggerFactory.getLogger(OmniPipelinedSubpartitionView.class);
    
    private PipelinedSubpartition pipelinedSubpartition;
    private long nativeTaskRef;
    private LocalInputChannel localInputChannel;
    private long localChannelReaderRef = -1;
    private int retryCount = 3;
    private ByteBuffer outputBufferStatus;
    private ByteBuffer outputBuffer;
    private long outputBufferAddress;
    private long statusAddress;
    // memorySegment address(8) + readIndex(4) + resultLength(4)+ currentDataType(4)+ bufferInbacklog(4) +
    // nextDataType(4) + int sequenceNumber(4) =  32, powerOfTwo(32) = 32
    
    private int outputBufferCapacity = 32;
    
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile boolean fetchingDataAvailableThreadRunning = true;
    
    
    public OmniPipelinedSubpartitionView(PipelinedSubpartition parent, BufferAvailabilityListener listener,
            long nativeTaskRef, int subPartitionIndex) throws PartitionNotFoundException {
        super(parent, listener);
        initDirectBuffers();
        localInputChannel = (LocalInputChannel) listener;
        this.pipelinedSubpartition = parent;
        this.nativeTaskRef = nativeTaskRef;
        ResultPartitionIDPOJO resultPartitionIDPOJO = new ResultPartitionIDPOJO(localInputChannel.getPartitionId());
        JSONObject jsonObject = new JSONObject(resultPartitionIDPOJO);
        String parititonIdString = jsonObject.toString();
        LOG.info("OmniPipelinedSubpartitionView createLocalChannelReader nativeTaskRef: {}, partitionId: {}, " +
                "subPartitionIndex: {}", nativeTaskRef, parititonIdString, subPartitionIndex);
        tryCreateLocalChannelReader(parititonIdString, subPartitionIndex);
        if (localChannelReaderRef == -1) {
            throw new PartitionNotFoundException(localInputChannel.getPartitionId());
        }
        startFetchingDataAvailableThread();
        LOG.info("OmniPipelinedSubpartitionView created, nativeTaskRef: {}, localChannelReaderRef: {}",
                nativeTaskRef, localChannelReaderRef);
    }
    
    private void startFetchingDataAvailableThread() {
        executor.execute(() -> {
            Thread.currentThread().setName("FetchingDataAvailableThread-" + nativeTaskRef + "-" + localChannelReaderRef);
            while (fetchingDataAvailableThreadRunning) {
                if (localChannelReaderRef != -1) {
                    try {
                        checkIfDataAvailable();
                    } catch (Exception e) {
                        LOG.error("Fetching data available thread interrupted", e);
                        executor.shutdownNow();
                        fetchingDataAvailableThreadRunning = false;
                    }
                }
            }
            LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Fetching data available thread stopped for " + "nativeTaskRef: {}, localChannelReaderRef: {}", nativeTaskRef, localChannelReaderRef);
        });
    }
    
    
    private void initDirectBuffers() {
        this.outputBuffer = ByteBuffer.allocateDirect(this.outputBufferCapacity);
        this.outputBufferAddress = getByteBufferAddress(outputBuffer);
        
        ByteOrder nativeOrder = ByteOrder.nativeOrder();
        
        if (nativeOrder == ByteOrder.BIG_ENDIAN) {
            LOG.info("Native byte order is big-endian");
        } else
            if (nativeOrder == ByteOrder.LITTLE_ENDIAN) {
                LOG.info("Native byte order is little-endian");
            } else {
                LOG.info("Unknown byte order");
            }
    }
    
    private void checkIfDataAvailable() {
        
        if (doCheckIfDataAvailable(localChannelReaderRef)) {
            notifyDataAvailable();
        }
        
    }
    
    // GET data from jni
    public synchronized ResultSubpartition.BufferAndBacklog getNextBuffer() {
        int res = getNativeNextBuffer(localChannelReaderRef);
        if (res == 1) {
            return decodeReadBuffer();
        } else {
            return null;
        }
    }
    
    private void tryCreateLocalChannelReader(String partitionId, int subPartitionIndex) {
        while (localChannelReaderRef == -1 && retryCount-- > 0) {
            try {
                localChannelReaderRef = createLocalChannelReader(nativeTaskRef, partitionId, subPartitionIndex,
                        outputBufferAddress);
                if (localChannelReaderRef != -1) {
                    LOG.info("createLocalChannelReader success, nativeTaskRef: {}, partitionId: {}, " +
                            "subPartitionIndex: {}", nativeTaskRef, partitionId, subPartitionIndex);
                } else {
                    Thread.sleep(1000);
                    LOG.info("createLocalChannelReader failed, retrying... nativeTaskRef: {}, partitionId: {}, " +
                            "subPartitionIndex: {}", nativeTaskRef, partitionId, subPartitionIndex);
                }
            } catch (Throwable e) {
                LOG.error("createLocalChannelReader failed, nativeTaskRef: {}, partitionId: {}, subPartitionIndex: " + "{}",
                        nativeTaskRef, partitionId, subPartitionIndex, e);
            }
        }
    }
    
    
    @Override
    public void releaseAllResources() {
        super.releaseAllResources();
        if (localChannelReaderRef != -1) {
            NativeBufferRecycler.unRegisterInstance(localChannelReaderRef);
            LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>OmniPipelinedSubpartitionView " +
                    "releaseAllResources, nativeTaskRef: {}, localChannelReaderRef: " + "{}", nativeTaskRef,
                    localChannelReaderRef);
        }
        fetchingDataAvailableThreadRunning = false;
        if (localChannelReaderRef != -1) {
            releaseNativeResource(localChannelReaderRef);
        }
        executor.shutdown();
    }
    
    public PipelinedSubpartition getPipelinedSubpartition() {
        return pipelinedSubpartition;
    }
    
    
    private ResultSubpartition.BufferAndBacklog decodeReadBuffer() {
        try {
            outputBuffer.position(0);
            outputBuffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // 1. memorySegment address
            long address = outputBuffer.getLong();
            if (address == -1) {
                // no data is available
                return null;
            } else {
                // 2.  readIndex(4)
                int readIndex = outputBuffer.getInt();
                // 3. resultLength(4)
                int length = outputBuffer.getInt();
                // 3. currentDataType(4)
                int currentDataType = outputBuffer.getInt();
                // 4. bufferInbacklog(4)
                int bufferInBacklog = outputBuffer.getInt();
                // 5.nextDataType(4)
                int nextDataType = outputBuffer.getInt();
                // 6. int sequenceNumber(4)
                int sequenceNumber = outputBuffer.getInt();
                
                // 6. wrap the memory segment
                ByteBuffer resultBuffer = MemoryUtils.wrapUnsafeMemoryWithByteBuffer(address, readIndex + length);
                MemorySegment memorySegment = MemorySegmentFactory.wrapOffHeapMemory(resultBuffer);
                NativeBufferRecycler nativeBufferRecycler = NativeBufferRecycler.getInstance(localChannelReaderRef);
                nativeBufferRecycler.registerMemorySegment(memorySegment, address);
                
                NetworkBuffer buffer = new NetworkBuffer(memorySegment, nativeBufferRecycler);
                buffer.setDataType(Buffer.DataType.values()[currentDataType]);
                
                ReadOnlySlicedNetworkBuffer readOnlySlicedNetworkBuffer = buffer.readOnlySlice(readIndex, length);
                Buffer.DataType nextDataTypeEnum = Buffer.DataType.values()[nextDataType];
                return new ResultSubpartition.BufferAndBacklog(readOnlySlicedNetworkBuffer, bufferInBacklog,
                        nextDataTypeEnum, sequenceNumber);
                
            }
        } catch (Throwable e) {
            LOG.error("decodeReadBuffer failed, nativeTaskRef: {}, localChannelReaderRef: {}", nativeTaskRef,
                    localChannelReaderRef, e);
            return null;
        } finally {
            // reset the outputBuffer
            outputBuffer.clear();
        }
        

    }

    @Override
    public void resumeConsumption() {
        resumeConsumption(localChannelReaderRef);
        // always notify data available after resume consumption
        notifyDataAvailable();
    }
    
    public native long createLocalChannelReader(long nativeTaskRef, String partitionId, int subPartitionIndex,
            long returnDataAddress);
    public native int getNativeNextBuffer(long localChannelReaderRef);
    public native boolean doCheckIfDataAvailable(long localChannelReaderRef);
    public native void releaseNativeResource(long localChannelReaderRef);
    public native void resumeConsumption(long localChannelReaderRef);
}
