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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * StreamConfigPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CheckpointConfigPOJO {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointConfigPOJO.class);

    private String checkpointsDirectory = "";

    private String stateBackend;

    private String checkpointStorage = "";

    private int maxRetainedCheckpoint;

    private boolean asyncSnapshots;

    private boolean incrementalCheckpoints;

    private boolean localRecovery;

    private String localRecoveryTaskManagerStateRootDirs = "";

    private String savepointDirectory = "";

    private long fsSmallFileThreshold;

    private int fsWriteBufferSize;
    
    /**
     * get checkpoint configs to build CheckpointConfigPOJO
     *
     * @param configuration configuration of flink
     * @param streamGraph streamGraph
     */
    public CheckpointConfigPOJO(ReadableConfig configuration, StreamGraph streamGraph) {
        this.setStateBackend(
            streamGraph.getStateBackend() == null ? "HashMapStateBackend" : streamGraph.getStateBackend().getName());
        this.setCheckpointStorage(configuration.get(CheckpointingOptions.CHECKPOINT_STORAGE));
        this.setMaxRetainedCheckpoint(configuration.get(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS));
        // state.backend.async is deprecated , Checkpoints are always asynchronous
        this.setAsyncSnapshots(true);
        this.setIncrementalCheckpoints(configuration.get(CheckpointingOptions.INCREMENTAL_CHECKPOINTS));
        this.setLocalRecovery(configuration.get(CheckpointingOptions.LOCAL_RECOVERY));
        this.setLocalRecoveryTaskManagerStateRootDirs(
            configuration.get(CheckpointingOptions.LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS));
        this.setSavepointDirectory(configuration.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
        this.setCheckpointsDirectory(configuration.get(CheckpointingOptions.CHECKPOINTS_DIRECTORY));
        this.setFsSmallFileThreshold(configuration.get(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD).getBytes());
        this.setFsWriteBufferSize(configuration.get(CheckpointingOptions.FS_WRITE_BUFFER_SIZE));
    }

    public CheckpointConfigPOJO() {
    }

    public String getCheckpointsDirectory() {
        return checkpointsDirectory;
    }

    public void setCheckpointsDirectory(String checkpointsDirectory) {
        this.checkpointsDirectory = Objects.toString(checkpointsDirectory, "");
    }

    public String getStateBackend() {
        return stateBackend;
    }

    public void setStateBackend(String stateBackend) {
        this.stateBackend = stateBackend;
    }

    public String getCheckpointStorage() {
        return checkpointStorage;
    }

    public void setCheckpointStorage(String checkpointStorage) {
        this.checkpointStorage = Objects.toString(checkpointStorage, "");
    }

    public int getMaxRetainedCheckpoint() {
        return maxRetainedCheckpoint;
    }

    public void setMaxRetainedCheckpoint(int maxRetainedCheckpoint) {
        this.maxRetainedCheckpoint = maxRetainedCheckpoint;
    }

    public boolean getAsyncSnapshots() {
        return asyncSnapshots;
    }

    public void setAsyncSnapshots(boolean asyncSnapshots) {
        this.asyncSnapshots = asyncSnapshots;
    }

    public boolean getIncrementalCheckpoints() {
        return incrementalCheckpoints;
    }

    public void setIncrementalCheckpoints(boolean incrementalCheckpoints) {
        this.incrementalCheckpoints = incrementalCheckpoints;
    }

    public boolean getLocalRecovery() {
        return localRecovery;
    }

    public void setLocalRecovery(boolean localRecovery) {
        this.localRecovery = localRecovery;
    }

    public String getLocalRecoveryTaskManagerStateRootDirs() {
        return localRecoveryTaskManagerStateRootDirs;
    }

    public void setLocalRecoveryTaskManagerStateRootDirs(String localRecoveryTaskManagerStateRootDirs) {
        this.localRecoveryTaskManagerStateRootDirs = Objects.toString(localRecoveryTaskManagerStateRootDirs, "");
    }

    public String getSavepointDirectory() {
        return savepointDirectory;
    }

    public void setSavepointDirectory(String savepointDirectory) {
        this.savepointDirectory = Objects.toString(savepointDirectory, "");
    }

    public long getFsSmallFileThreshold() {
        return fsSmallFileThreshold;
    }

    public void setFsSmallFileThreshold(long fsSmallFileThreshold) {
        this.fsSmallFileThreshold = fsSmallFileThreshold;
    }

    public int getFsWriteBufferSize() {
        return fsWriteBufferSize;
    }

    public void setFsWriteBufferSize(int fsWriteBufferSize) {
        this.fsWriteBufferSize = fsWriteBufferSize;
    }
}
