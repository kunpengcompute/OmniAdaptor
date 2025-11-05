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

package com.huawei.omniruntime.flink.streaming.runtime.io;

import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;

import java.nio.ByteBuffer;

public interface BinaryDataOutput extends PushingAsyncDataInput.DataOutput {
    /** Writes the given serialized record to the target subpartition. */
    void emitRecord(ByteBuffer record, int targetSubpartition);
}