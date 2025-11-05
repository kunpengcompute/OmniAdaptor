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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NativeNetworkBufferRecycler extends NativeBufferRecycler {
    private static final Logger LOG = LoggerFactory.getLogger(NativeNetworkBufferRecycler.class);
    private static final ConcurrentHashMap<Long, NativeNetworkBufferRecycler> INSTANCE_MAP = new ConcurrentHashMap<>();
    
    
    private NativeNetworkBufferRecycler(long nativeReaderRef) {
        super(nativeReaderRef);
    }
    
    public static synchronized NativeBufferRecycler getInstance(long nativeReaderRef) {
        NativeNetworkBufferRecycler bufferRecycler = getInstanceByNativeReaderRef(nativeReaderRef);
        if (bufferRecycler == null) {
            bufferRecycler = createNativeBufferRecycler(nativeReaderRef);
            addRecycler(nativeReaderRef, bufferRecycler);
        }
        return bufferRecycler;
    }
    
    public synchronized static NativeNetworkBufferRecycler getInstanceByNativeReaderRef(long nativeReaderRef) {
        return INSTANCE_MAP.get(nativeReaderRef);
    }
    
    public synchronized static void addRecycler(long nativeReaderRef, NativeNetworkBufferRecycler recycler) {
        INSTANCE_MAP.put(nativeReaderRef, recycler);
    }
    
    public static void unRegisterInstance(long nativeReaderRef) {
        NativeNetworkBufferRecycler nativeNetworkBufferRecycler = INSTANCE_MAP.remove(nativeReaderRef);
        if (nativeNetworkBufferRecycler != null) {
            LOG.info("**** Unregister native buffer recycler instance and find there are {} memory need to be freed" +
                    ".", nativeNetworkBufferRecycler.memorySegmentToAddress.size());
            for (Map.Entry<MemorySegment, Long> entry :
                    nativeNetworkBufferRecycler.memorySegmentToAddress.entrySet()) {
                nativeNetworkBufferRecycler.freeNativeByteBuffer(nativeReaderRef, entry.getValue());
            }
            nativeNetworkBufferRecycler.memorySegmentToAddress.clear();
        }
    }
    
    public static NativeNetworkBufferRecycler createNativeBufferRecycler(long nativeReaderRef) {
        return new NativeNetworkBufferRecycler(nativeReaderRef);
    }
    
    public native void freeNativeByteBuffer(long nativeReaderRef, long address);
    
}
