/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.restore;

import java.util.List;

public class KeyGroupEntryWrapper {
    private KeyGroupEntry[] entries;
    private int currentKvStateId;
    private int kvStateId;
    private int count;

    public KeyGroupEntryWrapper(KeyGroupEntry[] entries, int currentKvStateId, int kvStateId, int count) {
        this.entries = entries;
        this.currentKvStateId = currentKvStateId;
        this.kvStateId = kvStateId;
        this.count = count;
    }

    public KeyGroupEntry[] getEntries() {
        return this.entries;
    }

    public int getCurrentKvStateId() {
        return this.currentKvStateId;
    }

    public int getKvStateId() {
        return this.kvStateId;
    }

    public int getCount() {
        return this.count;
    }
}