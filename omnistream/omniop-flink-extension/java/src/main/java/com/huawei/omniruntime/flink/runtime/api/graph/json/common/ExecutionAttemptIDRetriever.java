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

package com.huawei.omniruntime.flink.runtime.api.graph.json.common;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraphID;

import java.lang.reflect.Field;

/**
 * ExecutionAttemptIDRetriever
 *
 * @version 1.0.0
 * @since 2025/02/20
 */
public class ExecutionAttemptIDRetriever {
    /**
     * getExecutionAttemptId
     *
     * @param executionAttemptID executionAttemptID
     * @return AbstractID
     */
    public static ExecutionGraphID getExecutionGraphId(ExecutionAttemptID executionAttemptID) {
        if (executionAttemptID == null) {
            return null;
        }
        try {
            Field field = ExecutionAttemptID.class.getDeclaredField("executionGraphId");
            field.setAccessible(true);
            Object val = field.get(executionAttemptID);
            if (!(val instanceof ExecutionGraphID)) {
                throw new IllegalStateException("executionGraphId is not an ExecutionGraphID");
            }
            return (ExecutionGraphID) val;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Error accessing executionGraphId: " + e.getMessage(), e);
        }
    }
}
