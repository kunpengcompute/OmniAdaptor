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

package com.huawei.omniruntime.flink.runtime.api.graph.json.common;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

/**
 * IntermediateDataSetIDPOJO
 *
 * @version 1.0.0
 * @since 2025/03/03
 */
public class IntermediateDataSetIDPOJO extends AbstractIDPOJO {
    public IntermediateDataSetIDPOJO() {
        super();
    }

    public IntermediateDataSetIDPOJO(long upperPart, long lowerPart) {
        super(upperPart, lowerPart);
    }

    public IntermediateDataSetIDPOJO(IntermediateDataSetID id) {
        super(id);
    }
}
