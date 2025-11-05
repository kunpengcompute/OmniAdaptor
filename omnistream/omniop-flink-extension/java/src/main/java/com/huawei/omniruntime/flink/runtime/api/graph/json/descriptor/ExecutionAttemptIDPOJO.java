package com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor;

import com.huawei.omniruntime.flink.runtime.api.graph.json.common.ExecutionAttemptIDRetriever;
import com.huawei.omniruntime.flink.runtime.api.graph.json.common.ExecutionGraphIDPOJO;
import com.huawei.omniruntime.flink.runtime.api.graph.json.common.ExecutionVertexIDPOJO;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraphID;

import java.io.Serializable;

/**
 * ExecutionAttemptIDPOJO
 *
 * @version 1.0.0
 * @since 2025/02/20
 */
public class ExecutionAttemptIDPOJO implements Serializable {
    private static final long serialVersionUID = -2377710916486194977L;

    private ExecutionGraphIDPOJO executionGraphId;
    private ExecutionVertexIDPOJO executionVertexId;
    private int attemptNumber;

    public ExecutionAttemptIDPOJO() {
        // Default constructor
    }

    public ExecutionAttemptIDPOJO(ExecutionAttemptID executionAttemptID) {
        ExecutionGraphID executionGraphID = ExecutionAttemptIDRetriever.getExecutionGraphId(executionAttemptID);
        this.executionGraphId = new ExecutionGraphIDPOJO(executionGraphID);
        this.executionVertexId = new ExecutionVertexIDPOJO(executionAttemptID.getExecutionVertexId());
        this.attemptNumber = executionAttemptID.getAttemptNumber();
    }


    public ExecutionAttemptIDPOJO(ExecutionGraphIDPOJO executionGraphId,
                                  ExecutionVertexIDPOJO executionVertexId,
                                  int attemptNumber) {
        this.executionGraphId = executionGraphId;
        this.executionVertexId = executionVertexId;
        this.attemptNumber = attemptNumber;
    }

    public ExecutionGraphIDPOJO getExecutionGraphId() {
        return executionGraphId;
    }

    public void setExecutionGraphId(ExecutionGraphIDPOJO executionGraphId) {
        this.executionGraphId = executionGraphId;
    }

    public ExecutionVertexIDPOJO getExecutionVertexId() {
        return executionVertexId;
    }

    public void setExecutionVertexId(ExecutionVertexIDPOJO executionVertexId) {
        this.executionVertexId = executionVertexId;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public void setAttemptNumber(int attemptNumber) {
        this.attemptNumber = attemptNumber;
    }

    @Override
    public String toString() {
        return "ExecutionAttemptIDPOJO{" +
                "executionGraphId=" + executionGraphId +
                ", executionVertexId=" + executionVertexId +
                ", attemptNumber=" + attemptNumber +
                '}';
    }

}
