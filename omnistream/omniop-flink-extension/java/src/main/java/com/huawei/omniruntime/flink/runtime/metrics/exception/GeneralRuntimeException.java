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

package com.huawei.omniruntime.flink.runtime.metrics.exception;

/**
 * GeneralRuntimeException is a custom runtime exception class that extends RuntimeException.
 * It provides constructors to create exceptions with a message, a cause, or both.
 * This class is used to handle general runtime errors in the application.
 *
 * @since 2025-04-29
 */
public class GeneralRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public GeneralRuntimeException(String message) {
        super(message);
    }

    public GeneralRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public GeneralRuntimeException(Throwable cause) {
        super(cause);
    }
}
