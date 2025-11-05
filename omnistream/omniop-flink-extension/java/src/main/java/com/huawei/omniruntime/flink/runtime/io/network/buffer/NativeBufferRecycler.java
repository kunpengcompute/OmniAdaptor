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

package com.huawei.omniruntime.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NativeBufferRecycler
 *
 * @version 1.0.0
 * @since 2025/03/14
 */

public class NativeBufferRecycler implements BufferRecycler {
    private static final Logger LOG = LoggerFactory.getLogger(NativeBufferRecycler.class);
    private static final ConcurrentHashMap<Long, NativeBufferRecycler> INSTANCE_MAP = new ConcurrentHashMap<>();

    protected Map<MemorySegment, Long> memorySegmentToAddress = new HashMap<>();
    private long nativeReaderRef;

    NativeBufferRecycler(long nativeReaderRef) {
        this.nativeReaderRef = nativeReaderRef;
    }

    /**
     * getInstance
     *
     * @param nativeReaderRef nativeReaderRef
     * @return NativeBufferRecycler
     */
    public static synchronized NativeBufferRecycler getInstance(long nativeReaderRef) {
        NativeBufferRecycler bufferRecycler = getInstanceByNativeReaderRef(nativeReaderRef);
        if (bufferRecycler == null) {
            bufferRecycler =  createNativeBufferRecycler(nativeReaderRef);
            addRecycler(nativeReaderRef, bufferRecycler);
        }
        return bufferRecycler;
    }
    
    public static NativeBufferRecycler createNativeBufferRecycler(long nativeReaderRef) {
        return new NativeBufferRecycler(nativeReaderRef);
    }

    @Override
    public void recycle(MemorySegment memorySegment) {
        Long address = memorySegmentToAddress.remove(memorySegment);
        if (address != null) {
            freeNativeByteBuffer(nativeReaderRef, address);
        }
    }

    /**
     * registerMemorySegment
     *
     * @param memorySegment memorySegment
     * @param address address
     */
    public void registerMemorySegment(MemorySegment memorySegment, long address) {
        memorySegmentToAddress.put(memorySegment, address);
    }

    /**
     * unRegisterInstance
     *
     * @param nativeReaderRef nativeReaderRef
     */
    public static void unRegisterInstance(long nativeReaderRef) {
        NativeBufferRecycler nativeBufferRecycler =
                INSTANCE_MAP.remove(nativeReaderRef);
        if (nativeBufferRecycler != null) {
            LOG.info("**** Unregister native buffer recycler instance and find there are {} memory need to be freed.",
                    nativeBufferRecycler.memorySegmentToAddress.size());
            for (Map.Entry<MemorySegment, Long> entry : nativeBufferRecycler.memorySegmentToAddress.entrySet()) {
                nativeBufferRecycler.freeNativeByteBuffer(nativeReaderRef,
                        entry.getValue());
            }
            nativeBufferRecycler.memorySegmentToAddress.clear();
        }
    }

    public synchronized  static void addRecycler(long nativeReaderRef, NativeBufferRecycler recycler) {
        INSTANCE_MAP.put(nativeReaderRef, recycler);
    }
    
    public synchronized static NativeBufferRecycler getInstanceByNativeReaderRef(long nativeReaderRef) {
        return INSTANCE_MAP.get(nativeReaderRef);
    }

    /**
     * freeNativeByteBuffer
     *
     * @param address address
     * @param nativeReaderRef nativeReaderRef
     */
    public native void freeNativeByteBuffer(long nativeReaderRef, long address);
}
