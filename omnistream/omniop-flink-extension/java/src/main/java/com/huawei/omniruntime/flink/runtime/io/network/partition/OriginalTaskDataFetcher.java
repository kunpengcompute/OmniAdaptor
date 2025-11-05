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

import org.apache.flink.runtime.io.network.partition.consumer.OmniLocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.OmniRemoteInputChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * OriginalTaskDataFetcher
 *
 * @version 1.0.0
 * @since 2025/04/25
 */
public class OriginalTaskDataFetcher {
    /**
     * LOG
     */
    private static final Logger LOG = LoggerFactory.getLogger(OriginalTaskDataFetcher.class);
    private long nativeTaskRef;
    private String taskName;
    private JobType jobType;
    private RemoteDataFetcher remoteDataFetcher;
    private LocalNativeDataFetcher localNativeDataFetcher;
    
    public OriginalTaskDataFetcher(long nativeTaskRef, String taskName, JobType jobType) {
        this.nativeTaskRef = nativeTaskRef;
        this.taskName = taskName;
        this.jobType = jobType;
    }
    
    /**
     * finishRunning
     */
    public void finishRunning() {
        if (remoteDataFetcher != null) {
            remoteDataFetcher.finishRunning();
        }
        
        if (localNativeDataFetcher != null) {
            localNativeDataFetcher.finishRunning();
        }
    }
    
    public void createAndStartRemoteDataFetcher(List<OmniRemoteInputChannel> remoteInputChannels) {
        if (jobType == JobType.SQL) {
            remoteDataFetcher = new RemoteDataFetcher(nativeTaskRef, taskName, jobType, remoteInputChannels);
        } else
            if (jobType == JobType.STREAM) {
                remoteDataFetcher = new DataStreamRemoteDataFetcher(nativeTaskRef, taskName, jobType,
                        remoteInputChannels);
            } else {
                LOG.error("Unsupported job type: {}", jobType);
                throw new IllegalArgumentException("Unsupported job type: " + jobType);
            }
        remoteDataFetcher.start();
    }
    
    public void createAndStartLocalDataFetcher(List<OmniLocalInputChannel> localInputChannels) throws IOException {
        localNativeDataFetcher = new LocalNativeDataFetcher(nativeTaskRef, taskName, localInputChannels);
        localNativeDataFetcher.start();
    }
}
