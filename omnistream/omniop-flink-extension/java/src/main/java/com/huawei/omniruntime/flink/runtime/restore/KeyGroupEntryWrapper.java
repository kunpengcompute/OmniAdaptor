/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.restore;

import java.util.List;

public class KeyGroupEntryWrapper {
    private List<KeyGroupEntry> entries;

    private int currentKvStateId;

    public KeyGroupEntryWrapper(List<KeyGroupEntry> entries, int currentKvStateId) {
        this.entries = entries;
        this.currentKvStateId = currentKvStateId;
    }

    public List<KeyGroupEntry> getEntries() {
        return this.entries;
    }

    public int getCurrentKvStateId() {
        return this.currentKvStateId;
    }
}