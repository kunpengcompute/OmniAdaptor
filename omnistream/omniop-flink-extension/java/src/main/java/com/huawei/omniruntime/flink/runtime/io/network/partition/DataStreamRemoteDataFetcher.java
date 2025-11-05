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

import com.huawei.omniruntime.flink.streaming.api.graph.JobType;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.OmniRemoteInputChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DataStreamRemoteDataFetcher extends RemoteDataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamRemoteDataFetcher.class);
    
    /**
     * DataStreamRemoteDataFetcher constructor
     *
     * @param nativeTaskRef nativeTaskRef
     * @param remoteInputChannels remoteInputChannels
     * @param taskName taskName
     * @param jobType jobType
     */
    public DataStreamRemoteDataFetcher(long nativeTaskRef, String taskName, JobType jobType,
            List<OmniRemoteInputChannel> remoteInputChannels) {
        super(nativeTaskRef, taskName, jobType, remoteInputChannels);
    }
    
    public void recycleBuffer(long address, Buffer buffer) {
        if (address != -1 && buffer != null) {
            waitingForRecycleBuffers.put(address, buffer);
            LOG.info("Buffer with address {} added to waiting for recycle buffers.", address);
        } else {
            LOG.warn("Attempted to recycle a null buffer or with invalid address: {}", address);
        }
    }
    
    
}
