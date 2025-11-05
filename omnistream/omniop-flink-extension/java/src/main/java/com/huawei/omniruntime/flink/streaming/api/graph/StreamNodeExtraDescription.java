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

package com.huawei.omniruntime.flink.streaming.api.graph;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

/**
 * basic interface for validate execution plan
 *
 * @since 2025/05/06
 */
public interface StreamNodeExtraDescription {
    /**
     * check & set the description
     *
     * @param streamNode   streamNode
     * @param streamConfig streamConfig
     * @return Boolean Boolean
     */
    boolean setExtraDescription(StreamNode streamNode, StreamConfig streamConfig, StreamGraph streamGraph, JobType jobType) throws Exception;
}
