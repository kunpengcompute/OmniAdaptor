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
 * NonPartitionWriter
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class NonPartitionWriter implements PartitionWriterDelegate {
    @Override
    public ResultPartitionWriter getPartitionWriter(int outputIndex) {
        throw new UnsupportedOperationException("No partition writer instance.");
    }
}
