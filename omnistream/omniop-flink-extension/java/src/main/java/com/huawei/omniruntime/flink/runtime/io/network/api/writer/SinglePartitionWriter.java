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

package com.huawei.omniruntime.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;

/**
 * SinglePartitionWriter
 *
 * @version 1.0.0
 * @since 2025/01/23
 */

public class SinglePartitionWriter implements PartitionWriterDelegate {
    private final ResultPartitionWriter writer;

    public SinglePartitionWriter(ResultPartitionWriter writer) {
        this.writer = writer;
    }


    @Override
    public ResultPartitionWriter getPartitionWriter(int outputIndex) {
        return this.writer;
    }
}
