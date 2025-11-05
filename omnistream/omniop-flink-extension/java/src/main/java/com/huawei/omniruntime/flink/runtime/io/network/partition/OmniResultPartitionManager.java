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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OmniResultPartitionManager
 *
 * @version 1.0.0
 * @since 2025/04/25
 */

public class OmniResultPartitionManager {
    private static final Logger LOG = LoggerFactory.getLogger(OmniResultPartitionManager.class);

    private long nativeResultPartitionManagerAddress;

    /**
     * registerResultPartition
     *
     * @param partition partition
     */
    public void registerResultPartition(OmniResultPartition partition) {
        long partitionAddress = partition.getNativeResultPartitionAddress();

        LOG.info("Registered {}.", partition);

        registerResultPartitionNative(nativeResultPartitionManagerAddress, partitionAddress);
        /**
         * synchronized (registeredPartitions) {
         * checkState(!isShutdown, "Result partition manager already shut down.");
         *
         * ResultPartition previous =
         * registeredPartitions.put(partition.getPartitionId(), partition);
         *
         * if (previous != null) {
         * throw new IllegalStateException("Result partition already registered.");
         * }
         *
         * LOG.debug("Registered {}.", partition);
         * } **/
    }

    private native void registerResultPartitionNative(
            long nativeResultPartitionManagerAddress,
            long nativePartitionAddress);
}
