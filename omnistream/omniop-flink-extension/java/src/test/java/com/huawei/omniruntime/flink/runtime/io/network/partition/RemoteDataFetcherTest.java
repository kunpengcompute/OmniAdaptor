/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.io.network.partition;

import com.huawei.omniruntime.flink.streaming.api.graph.JobType;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemoteDataFetcherTest {
    @Test
    void finishRunningCompletesWhenCallerIsInterrupted() {
        RemoteDataFetcher fetcher =
                new RemoteDataFetcher(0, "cancel-test", JobType.SQL, Collections.emptyList());

        Thread.currentThread().interrupt();
        try {
            assertDoesNotThrow(fetcher::finishRunning);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
