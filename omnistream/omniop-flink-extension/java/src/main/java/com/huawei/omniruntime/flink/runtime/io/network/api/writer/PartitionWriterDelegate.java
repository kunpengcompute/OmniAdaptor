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

package com.huawei.omniruntime.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;

/**
 * The partition writer delegate provides the availability function for task processor, and it might
 * represent a single
 * {@link ResultPartitionWriter}
 * or multiple
 * {@link ResultPartitionWriter}
 * instances in specific
 * implementations.
 *
 * @version 1.0.0
 * @since 2025/04/24
 */
public interface PartitionWriterDelegate {
    /**
     * Returns the internal actual partittion writer instance based on the output index.
     *
     * @param outputIndex the index respective to the record writer instance.
     * @return {@link ResultPartitionWriter }
     */
    ResultPartitionWriter getPartitionWriter(int outputIndex);
}
