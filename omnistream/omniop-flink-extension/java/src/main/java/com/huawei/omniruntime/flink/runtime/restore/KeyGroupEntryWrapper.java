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
