/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * OmniAdaptor is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
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
}
