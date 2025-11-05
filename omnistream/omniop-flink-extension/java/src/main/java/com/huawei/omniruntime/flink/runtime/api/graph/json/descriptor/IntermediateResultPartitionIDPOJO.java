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

package com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor;

import com.huawei.omniruntime.flink.runtime.api.graph.json.common.AbstractIDPOJO;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

/**
 * IntermediateResultPartitionIDPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class IntermediateResultPartitionIDPOJO {
    private AbstractIDPOJO intermediateDataSetID;
    private int partitionNum;

    public IntermediateResultPartitionIDPOJO() {
    }

    public IntermediateResultPartitionIDPOJO(IntermediateResultPartitionID intermediateResultPartitionID) {
        this.partitionNum = intermediateResultPartitionID.getPartitionNumber();
        this.intermediateDataSetID =
                new AbstractIDPOJO(intermediateResultPartitionID.getIntermediateDataSetID().getUpperPart(),
                intermediateResultPartitionID.getIntermediateDataSetID().getLowerPart());
    }

    public IntermediateResultPartitionIDPOJO(AbstractIDPOJO intermediateDataSetID, int partitionNum) {
        this.intermediateDataSetID = intermediateDataSetID;
        this.partitionNum = partitionNum;
    }

    public AbstractIDPOJO getIntermediateDataSetID() {
        return intermediateDataSetID;
    }

    public void setIntermediateDataSetID(AbstractIDPOJO intermediateDataSetID) {
        this.intermediateDataSetID = intermediateDataSetID;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    @Override
    public String toString() {
        return "IntermediateResultPartitionIDPOJO{"
                + "intermediateDataSetID=" + intermediateDataSetID
                + ", partitionNum=" + partitionNum
                + '}';
    }
}