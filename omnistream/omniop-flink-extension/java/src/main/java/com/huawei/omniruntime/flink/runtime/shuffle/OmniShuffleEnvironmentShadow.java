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

package com.huawei.omniruntime.flink.runtime.shuffle;

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * OmniShuffleEnvironmentShadow
 *
 * @since 2025-04-27
 */
public class OmniShuffleEnvironmentShadow implements OmniShuffleEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(OmniShuffleEnvironmentShadow.class);

    private long nativeShuffleEnvironmentAddress;


    public OmniShuffleEnvironmentShadow(long nativeShuffleServiceRef) {
        this.nativeShuffleEnvironmentAddress = nativeShuffleServiceRef;
    }


    public long getNativeShuffleEnvironmentAddress() {
        return nativeShuffleEnvironmentAddress;
    }

    @Override
    public long getNativeShuffleServiceRef() {
        return 0;
    }


    @Override
    public OmniShuffleIOOwnerContext createShuffleIOOwnerContext(
            String ownerName, ExecutionAttemptID executionAttemptID) {
        return new OmniShuffleIOOwnerContext(
                checkNotNull(ownerName),
                checkNotNull(executionAttemptID));
    }


    @Override
    public List createResultPartitionWriters(OmniShuffleIOOwnerContext ownerContext,
                                             List<ResultPartitionDeploymentDescriptor>
                                                     resultPartitionDeploymentDescriptors) {
        // call native implemeantion
        return null;
    }
}
