/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.restore;

public class KeyGroupEntry {
    private final byte[] key;
    private final byte[] value;

    public KeyGroupEntry(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        return this.key;
    }

    public byte[] getValue() {
        return this.value;
    }
}