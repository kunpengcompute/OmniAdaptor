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

package com.huawei.omniruntime.flink.runtime.shuffle;

import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.List;

// TBD
/**
 * OmniShuffleEnvironment
 *
 * @since 2025-04-27
 */
public interface OmniShuffleEnvironment {
    /**
     * getNativeShuffleServiceRef
     *
     * @return long
     */
    long getNativeShuffleServiceRef();

    /**
     * createShuffleIOOwnerContext
     *
     * @param taskNameWithSubtaskAndId taskNameWithSubtaskAndId
     * @param executionId executionId
     * @return OmniShuffleIOOwnerContext
     */
    OmniShuffleIOOwnerContext createShuffleIOOwnerContext(
            String taskNameWithSubtaskAndId,
            ExecutionAttemptID executionId);

    /**
     * createResultPartitionWriters
     *
     * @param ownerContext ownerContext
     * @param resultPartitionDeploymentDescriptors resultPartitionDeploymentDescriptors
     * @return List
     */
    List createResultPartitionWriters(
            OmniShuffleIOOwnerContext ownerContext,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors);
}
