/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.restore;

public class KeyGroupEntry {
    private final int kvStateId;
    private final int[] key;
    private final int[] value;

    public KeyGroupEntry(int kvStateId, int[] key, int[] value) {
        this.kvStateId = kvStateId;
        this.key = key;
        this.value = value;
    }

    public int getKvStateId() {
        return this.kvStateId;
    }

    public int[] getKey() {
        return this.key;
    }

    public int[] getValue() {
        return this.value;
    }
}