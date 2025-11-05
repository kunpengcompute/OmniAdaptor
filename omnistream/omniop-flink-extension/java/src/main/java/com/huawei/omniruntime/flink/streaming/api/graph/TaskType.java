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

public enum TaskType {
    NULL(0),
    SQL(1),
    STREAM(2),
    SQL_STREAM(3);
    private final int value;

    TaskType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static TaskType fromValue(int value) {
        for (TaskType taskType : TaskType.values()) {
            if (taskType.value == value) {
                return taskType;
            }
        }
        throw new IllegalArgumentException("Invalid value: " + value);
    }

    public TaskType getCombinationsOperatorType(OperatorType operatorType) {
        return fromValue(this.getValue() | operatorType.getValue());
    }
}
