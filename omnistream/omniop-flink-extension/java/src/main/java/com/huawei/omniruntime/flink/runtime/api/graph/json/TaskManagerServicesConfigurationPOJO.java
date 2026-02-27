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

package com.huawei.omniruntime.flink.runtime.api.graph.json;

import com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor.ResourceIDPOJO;
import com.huawei.omniruntime.flink.runtime.taskexecutor.OmniTaskManagerServices;

import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TaskManagerServicesConfigurationPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class TaskManagerServicesConfigurationPOJO {
    private static final Logger LOG = LoggerFactory.getLogger(OmniTaskManagerServices.class);

    private ResourceIDPOJO resourceID;
    private long memorySize;
    private int pageSize;
    private long requestSegmentsTimeoutMillis;
    private int numIoThreads;
    private String externalAddress;
    private boolean localCommunicationOnly;
    private int networkbuffersPerChannel;
    private int partitionRequestMaxBackoff;
    private int partitionRequestInitialBackoff;
    private int floatingNetworkbuffersPerGate;
    private int segmentSize;
    private int numberofSegmentsGlobal;
    private int sortShuffleMinBuffers;
    private int sortShuffleMinParallelism;
    private int maxBuffersPerChannel;

    public TaskManagerServicesConfigurationPOJO(ResourceIDPOJO resourceID, long memorySize,
                                                int pageSize, long requestSegmentsTimeoutMillis,
                                                int numIoThreads, String externalAddress,
                                                boolean localCommunicationOnly,
                                                NettyShuffleEnvironmentConfiguration networkConfig) {
        this.resourceID = resourceID;
        this.memorySize = memorySize;
        this.pageSize = pageSize;
        this.requestSegmentsTimeoutMillis = requestSegmentsTimeoutMillis;
        this.numIoThreads = numIoThreads;
        this.externalAddress = externalAddress;
        this.localCommunicationOnly = localCommunicationOnly;

        this.networkbuffersPerChannel = networkConfig.networkBuffersPerChannel();
        this.partitionRequestMaxBackoff = networkConfig.partitionRequestMaxBackoff();
        this.partitionRequestInitialBackoff = networkConfig.partitionRequestInitialBackoff();
        this.floatingNetworkbuffersPerGate = networkConfig.floatingNetworkBuffersPerGate();
        this.segmentSize = networkConfig.networkBufferSize();
        this.numberofSegmentsGlobal = networkConfig.numNetworkBuffers();
        this.sortShuffleMinBuffers = networkConfig.sortShuffleMinBuffers();
        this.sortShuffleMinParallelism = networkConfig.sortShuffleMinParallelism();
        this.maxBuffersPerChannel = networkConfig.getMaxBuffersPerChannel();
    }

    public TaskManagerServicesConfigurationPOJO() {
    }

    public ResourceIDPOJO getResourceID() {
        return resourceID;
    }

    public void setResourceID(ResourceIDPOJO resourceID) {
        this.resourceID = resourceID;
    }

    public String getExternalAddress() {
        return externalAddress;
    }

    public void setExternalAddress(String externalAddress) {
        this.externalAddress = externalAddress;
    }

    public boolean isLocalCommunicationOnly() {
        return localCommunicationOnly;
    }

    public void setLocalCommunicationOnly(boolean localCommunicationOnly) {
        this.localCommunicationOnly = localCommunicationOnly;
    }

    public long getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public long getMemorySize() {
        return memorySize;
    }

    public void setMemorySize(long memorySize) {
        this.memorySize = memorySize;
    }

    public long getRequestSegmentsTimeoutMillis() {
        return requestSegmentsTimeoutMillis;
    }

    public void setRequestSegmentsTimeoutMillis(long requestSegmentsTimeoutMillis) {
        this.requestSegmentsTimeoutMillis = requestSegmentsTimeoutMillis;
    }

    public int getNumIoThreads() {
        return numIoThreads;
    }

    public void setNumIoThreads(int numIoThreads) {
        this.numIoThreads = numIoThreads;
    }

    public int getNetworkbuffersPerChannel() {
        return networkbuffersPerChannel;
    }

    public void setNetworkbuffersPerChannel(int networkbuffersPerChannel) {
        this.networkbuffersPerChannel = networkbuffersPerChannel;
    }

    public int getPartitionRequestMaxBackoff() {
        return partitionRequestMaxBackoff;
    }

    public void setPartitionRequestMaxBackoff(int partitionRequestMaxBackoff) {
        this.partitionRequestMaxBackoff = partitionRequestMaxBackoff;
    }

    public int getPartitionRequestInitialBackoff() {
        return partitionRequestInitialBackoff;
    }

    public void setPartitionRequestInitialBackoff(int partitionRequestInitialBackoff) {
        this.partitionRequestInitialBackoff = partitionRequestInitialBackoff;
    }

    public int getFloatingNetworkbuffersPerGate() {
        return floatingNetworkbuffersPerGate;
    }

    public void setFloatingNetworkbuffersPerGate(int floatingNetworkbuffersPerGate) {
        this.floatingNetworkbuffersPerGate = floatingNetworkbuffersPerGate;
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    public void setSegmentSize(int segmentSize) {
        this.segmentSize = segmentSize;
    }

    public int getNumberofSegmentsGlobal() {
        return numberofSegmentsGlobal;
    }

    public void setNumberofSegmentsGlobal(int numberofSegmentsGlobal) {
        this.numberofSegmentsGlobal = numberofSegmentsGlobal;
    }

    @Override
    public String toString() {
        return "TaskManagerServicesConfigurationPOJO{"
                + "numIoThreads=" + numIoThreads
                + ", resourceID=" + resourceID
                + ", externalAddress='" + externalAddress + '\''
                + ", localCommunicationOnly=" + localCommunicationOnly
                + ", pageSize=" + pageSize
                + '}';
    }

    public int getSortShuffleMinBuffers() {
        return sortShuffleMinBuffers;
    }

    public int getSortShuffleMinParallelism() {
        return sortShuffleMinParallelism;
    }

    public void setSortShuffleMinBuffers(int sortShuffleMinBuffers) {
        this.sortShuffleMinBuffers = sortShuffleMinBuffers;
    }

    public void setSortShuffleMinParallelism(int sortShuffleMinParallelism) {
        this.sortShuffleMinParallelism = sortShuffleMinParallelism;
    }

    public int getMaxBuffersPerChannel() {
        return maxBuffersPerChannel;
    }

    public void setMaxBuffersPerChannel(int maxBuffersPerChannel) {
        this.maxBuffersPerChannel = maxBuffersPerChannel;
    }
}
