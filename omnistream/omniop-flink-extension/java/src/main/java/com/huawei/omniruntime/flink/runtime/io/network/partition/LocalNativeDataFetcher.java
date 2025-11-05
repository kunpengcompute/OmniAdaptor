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

import org.apache.flink.runtime.io.network.partition.consumer.OmniLocalInputChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LocalNativeDataFetcher implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LocalNativeDataFetcher.class);
    private List<OmniLocalInputChannel> localInputChannels;
    private long nativeTaskRef;
    private String taskName;
    private volatile boolean running = true;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    
    public LocalNativeDataFetcher(long nativeTaskRef, String taskName,
            List<OmniLocalInputChannel> localInputChannels) {
        this.nativeTaskRef = nativeTaskRef;
        this.taskName = taskName;
        this.localInputChannels = localInputChannels;
    }
    
    private void changeNativeLocalInputChannelToOriginal() throws IOException {
        if (localInputChannels != null && localInputChannels.size() > 0) {
            for (OmniLocalInputChannel omniLocalInputChannel : localInputChannels) {
                if (!omniLocalInputChannel.changeNativeLocalInputChannel()) {
                    LOG.error("Failed to change native local input channel to original local input channel");
                    throw new IOException("Failed to change native local input channel to original local input " +
                                            "channel");
                } else {
                    omniLocalInputChannel.startRecycleBuffersThread();
                }
            }
        }
    }
    private void connectToOriginalPipelinedsubpartition() throws IOException {
        if (localInputChannels != null && localInputChannels.size() > 0) {
            for (OmniLocalInputChannel omniLocalInputChannel : localInputChannels) {
                try {
                    omniLocalInputChannel.doRequestSubpartition();
                } catch (IOException e) {
                    LOG.error("Failed to request subpartition for original local input channel", e);
                    throw e;
                }
            }
        }
    }
    
    public void connectCppLocalInputChannelToJavaPipelinedsubpartition() throws IOException {
        LOG.info("Connecting Cpp Local Input Channel to Java Pipelined Subpartition for task: {}", taskName);
        connectToOriginalPipelinedsubpartition();
        LOG.info("Successfully connected Cpp Local Input Channel to Java Pipelined Subpartition for task: {}",
                taskName);
    }
    
    public void start() throws IOException {
        changeNativeLocalInputChannelToOriginal();
        executorService.submit(this);
    }
    
    public void run() {
        try {
            Thread.currentThread().setName("LocalNativeDataFetcher-----> for task: " + taskName);
            LOG.info("Running LocalNativeDataFetcher for task: {}", taskName);
            connectCppLocalInputChannelToJavaPipelinedsubpartition();
        } catch (IOException e) {
            LOG.error("Error starting LocalNativeDataFetcher for task: {}", taskName, e);
        }
    }
    
    public void finishRunning() {
        for (OmniLocalInputChannel omniLocalInputChannel : localInputChannels) {
            try {
                omniLocalInputChannel.releaseAllResources();
            } catch (IOException e) {
                LOG.error("Error releasing resources for OmniLocalInputChannel", e);
            }
        }
    }
}
