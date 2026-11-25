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

package com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor;

import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

/**
 * ShuffleDescriptorPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class ShuffleDescriptorPOJO {
    private ResultPartitionIDPOJO resultPartitionID;
    private boolean isUnknown;
    private boolean hasLocalResource;
    private ResourceIDPOJO storesLocalResourcesOn;

    public ShuffleDescriptorPOJO() {
    }

    public ShuffleDescriptorPOJO(ShuffleDescriptor shuffleDescriptor) {
        this.resultPartitionID = new ResultPartitionIDPOJO(shuffleDescriptor.getResultPartitionID());
        this.isUnknown = shuffleDescriptor.isUnknown();
        this.hasLocalResource = shuffleDescriptor.storesLocalResourcesOn().isPresent();
        this.storesLocalResourcesOn = new ResourceIDPOJO(shuffleDescriptor.storesLocalResourcesOn());
    }

    public ShuffleDescriptorPOJO(
            ResultPartitionIDPOJO resultPartitionID,
            boolean isUnknown,
            boolean hasLocalResource,
            ResourceIDPOJO storesLocalResourcesOn) {
        this.resultPartitionID = resultPartitionID;
        this.isUnknown = isUnknown;
        this.hasLocalResource = hasLocalResource;
        this.storesLocalResourcesOn = storesLocalResourcesOn;
    }

    public ResultPartitionIDPOJO getResultPartitionID() {
        return resultPartitionID;
    }

    public void setResultPartitionID(ResultPartitionIDPOJO resultPartitionID) {
        this.resultPartitionID = resultPartitionID;
    }

    public boolean isUnknown() {
        return isUnknown;
    }

    public void setUnknown(boolean unknown) {
        isUnknown = unknown;
    }

    public boolean isHasLocalResource() {
        return hasLocalResource;
    }

    public void setHasLocalResource(boolean hasLocalResource) {
        this.hasLocalResource = hasLocalResource;
    }

    public ResourceIDPOJO getStoresLocalResourcesOn() {
        return storesLocalResourcesOn;
    }

    public void setStoresLocalResourcesOn(ResourceIDPOJO storesLocalResourcesOn) {
        this.storesLocalResourcesOn = storesLocalResourcesOn;
    }

    @Override
    public String toString() {
        return "ShuffleDescriptorPOJO{"
                + "resultPartitionID=" + resultPartitionID
                + ", isUnknown=" + isUnknown
                + ", hasLocalResource=" + hasLocalResource
                + ", storesLocalResourcesOn=" + storesLocalResourcesOn
                + '}';
    }
}
