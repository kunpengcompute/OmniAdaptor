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

package com.huawei.omniruntime.flink.runtime.api.state.serializer.model.entity;

import java.util.List;

public class VectorBatch {
    int flatSize = 0;
    long capacity;
    long rowCnt;
    List<Object> vectors;

    public int getFlatSize() {
        return flatSize;
    }

    public void setFlatSize(int flatSize) {
        this.flatSize = flatSize;
    }

    public long getCapacity() {
        return capacity;
    }

    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    public long getRowCnt() {
        return rowCnt;
    }

    public void setRowCnt(long rowCnt) {
        this.rowCnt = rowCnt;
    }

    public List<Object> getVectors() {
        return vectors;
    }

    public void setVectors(List<Object> vectors) {
        this.vectors = vectors;
    }
}
