package org.apache.flink.streaming.api.graph;

import com.huawei.omniruntime.flink.streaming.api.graph.JobType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskFallbackPolicyTest {
    @Test
    void dataStreamKeepsTaskFallbackRegardlessOfSqlSwitchOrRejectionReason() {
        for (boolean enabled : new boolean[] {false, true}) {
            for (boolean endpoint : new boolean[] {false, true}) {
                assertFalse(StreamingJobGraphGenerator.requiresWholeJobFallback(
                        JobType.STREAM, false, enabled, endpoint));
            }
        }
    }

    @Test
    void sqlOnlyAllowsEndpointFallbackWhenEnabled() {
        assertFalse(StreamingJobGraphGenerator.requiresWholeJobFallback(
                JobType.SQL, false, true, true));
        assertTrue(StreamingJobGraphGenerator.requiresWholeJobFallback(
                JobType.SQL, false, false, true));
        for (boolean enabled : new boolean[] {false, true}) {
            assertTrue(StreamingJobGraphGenerator.requiresWholeJobFallback(
                    JobType.SQL, false, enabled, false));
        }
    }

    @Test
    void supportedTasksNeverTriggerWholeJobFallback() {
        for (JobType type : new JobType[] {JobType.SQL, JobType.STREAM}) {
            for (boolean enabled : new boolean[] {false, true}) {
                assertFalse(StreamingJobGraphGenerator.requiresWholeJobFallback(
                        type, true, enabled, false));
            }
        }
    }
}
