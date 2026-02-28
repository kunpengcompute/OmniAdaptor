/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.restore;

public class KeyGroupEntry {
    private final int kvStateId;
    private final byte[] key;
    private final byte[] value;

    public KeyGroupEntry(int kvStateId, byte[] key, byte[] value) {
        this.kvStateId = kvStateId;
        this.key = key;
        this.value = value;
    }

    public int getKvStateId() {
        return this.kvStateId;
    }

    public byte[] getKey() {
        return this.key;
    }

    public byte[] getValue() {
        return this.value;
    }
}