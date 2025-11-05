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

package com.huawei.omniruntime.flink.runtime.api.graph.json;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamConfigPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExecutionCheckpointConfigPOJO {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionCheckpointConfigPOJO.class);

    private long alignedCheckpointTimeoutSecond =
        ExecutionCheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT.defaultValue().getSeconds();

    private long alignedCheckpointTimeoutNano =
        ExecutionCheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT.defaultValue().getNano();

    private long checkpointIdOfIgnoredInFlightData = -1;

    private long checkpointInterval = -1;

    private long checkpointTimeout = 10 * 60 * 1000;

    private CheckpointingMode checkpointingMode = CheckpointingMode.EXACTLY_ONCE;

    private CheckpointConfig.ExternalizedCheckpointCleanup externalizedCheckpointCleanup =
        ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT.defaultValue();

    private boolean forceUnalignedCheckpoints;

    private int maxConcurrentCheckpoints = 1;

    private long minPauseBetweenCheckpoints = 0L;

    private int tolerableCheckpointFailureNumber = -1;

    private boolean unalignedCheckpointsEnabled = true;

    private boolean checkpointAfterTasksFinishEnabled;

    /**
     * get execution checkpoint configs to build CheckpointConfigPOJO
     *
     * @param checkpointCfg CheckpointConfig
     * @param configuration configuration of flink
     */
    public ExecutionCheckpointConfigPOJO(CheckpointConfig checkpointCfg, ReadableConfig configuration) {
        this.setCheckpointingMode(checkpointCfg.getCheckpointingMode());
        this.setCheckpointTimeout(checkpointCfg.getCheckpointTimeout());
        this.setMaxConcurrentCheckpoints(checkpointCfg.getMaxConcurrentCheckpoints());
        this.setMinPauseBetweenCheckpoints(checkpointCfg.getMinPauseBetweenCheckpoints());
        this.setTolerableCheckpointFailureNumber(checkpointCfg.getTolerableCheckpointFailureNumber());
        this.setExternalizedCheckpointCleanup(checkpointCfg.getExternalizedCheckpointCleanup());
        this.setCheckpointInterval(checkpointCfg.getCheckpointInterval());
        this.setUnalignedCheckpointsEnabled(checkpointCfg.isUnalignedCheckpointsEnabled());
        this.setAlignedCheckpointTimeoutSecond(checkpointCfg.getAlignedCheckpointTimeout().getSeconds());
        this.setAlignedCheckpointTimeoutNano(checkpointCfg.getAlignedCheckpointTimeout().getNano());
        this.setForceUnalignedCheckpoints(checkpointCfg.isForceUnalignedCheckpoints());
        this.setCheckpointIdOfIgnoredInFlightData(checkpointCfg.getCheckpointIdOfIgnoredInFlightData());

        Boolean cpAfterFinish = configuration.get(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH);
        this.setCheckpointAfterTasksFinishEnabled(cpAfterFinish);
    }

    public ExecutionCheckpointConfigPOJO() {
    }

    public long getAlignedCheckpointTimeoutSecond() {
        return alignedCheckpointTimeoutSecond;
    }

    public void setAlignedCheckpointTimeoutSecond(long alignedCheckpointTimeout) {
        this.alignedCheckpointTimeoutSecond = alignedCheckpointTimeout;
    }

    public long getCheckpointIdOfIgnoredInFlightData() {
        return checkpointIdOfIgnoredInFlightData;
    }

    public void setCheckpointIdOfIgnoredInFlightData(long checkpointIdOfIgnoredInFlightData) {
        this.checkpointIdOfIgnoredInFlightData = checkpointIdOfIgnoredInFlightData;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(long checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    public long getCheckpointTimeout() {
        return checkpointTimeout;
    }

    public void setCheckpointTimeout(long checkpointTimeout) {
        this.checkpointTimeout = checkpointTimeout;
    }

    public CheckpointingMode getCheckpointingMode() {
        return checkpointingMode;
    }

    public void setCheckpointingMode(CheckpointingMode checkpointingMode) {
        this.checkpointingMode = checkpointingMode;
    }

    public CheckpointConfig.ExternalizedCheckpointCleanup getExternalizedCheckpointCleanup() {
        return externalizedCheckpointCleanup;
    }

    public void
        setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup externalizedCheckpointCleanup) {
        this.externalizedCheckpointCleanup = externalizedCheckpointCleanup;
    }

    public boolean getForceUnalignedCheckpoints() {
        return forceUnalignedCheckpoints;
    }

    public void setForceUnalignedCheckpoints(boolean forceUnalignedCheckpoints) {
        this.forceUnalignedCheckpoints = forceUnalignedCheckpoints;
    }

    public int getMaxConcurrentCheckpoints() {
        return maxConcurrentCheckpoints;
    }

    public void setMaxConcurrentCheckpoints(int maxConcurrentCheckpoints) {
        this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
    }

    public long getMinPauseBetweenCheckpoints() {
        return minPauseBetweenCheckpoints;
    }

    public void setMinPauseBetweenCheckpoints(long minPauseBetweenCheckpoints) {
        this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
    }

    public int getTolerableCheckpointFailureNumber() {
        return tolerableCheckpointFailureNumber;
    }

    public void setTolerableCheckpointFailureNumber(int tolerableCheckpointFailureNumber) {
        this.tolerableCheckpointFailureNumber = tolerableCheckpointFailureNumber;
    }

    public boolean getUnalignedCheckpointsEnabled() {
        return unalignedCheckpointsEnabled;
    }

    public void setUnalignedCheckpointsEnabled(boolean unalignedCheckpointsEnabled) {
        this.unalignedCheckpointsEnabled = unalignedCheckpointsEnabled;
    }

    public long getAlignedCheckpointTimeoutNano() {
        return alignedCheckpointTimeoutNano;
    }

    public void setAlignedCheckpointTimeoutNano(long alignedCheckpointTimeoutNano) {
        this.alignedCheckpointTimeoutNano = alignedCheckpointTimeoutNano;
    }

    public boolean getCheckpointAfterTasksFinishEnabled() {
        return checkpointAfterTasksFinishEnabled;
    }

    public void setCheckpointAfterTasksFinishEnabled(boolean checkpointAfterTasksFinishEnabled) {
        this.checkpointAfterTasksFinishEnabled = checkpointAfterTasksFinishEnabled;
    }
}
