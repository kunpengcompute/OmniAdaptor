package com.huawei.omniruntime.flink.runtime.api.graph.json;

import static org.apache.flink.util.Preconditions.checkState;

import org.apache.flink.api.common.JobID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * JobIDPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class JobIDPOJO {
    private static final Logger LOG = LoggerFactory.getLogger(JobIDPOJO.class);
    private final long upperPart;
    private final long lowerPart;

    public JobIDPOJO(long upperPart, long lowerPart) {
        this.upperPart = upperPart;
        this.lowerPart = lowerPart;
    }

    public JobIDPOJO(JobID jobId) {
        this(jobId.getUpperPart(), jobId.getLowerPart());
    }

    public static void main(String[] args) {
        JobIDPOJO jobID1 = new JobIDPOJO(12345L, 67890L);
        JobIDPOJO jobID2 = new JobIDPOJO(12345L, 67890L);
        JobIDPOJO jobID3 = new JobIDPOJO(99999L, 11111L);

        LOG.info("Job ID 1: {}", jobID1);
        LOG.info("Job ID 1 upper: {}", jobID1.getUpperPart());
        LOG.info("Job ID 1 lower: {}", jobID1.getLowerPart());

        LOG.info("jobID1 equals jobID2: {}", jobID1.equals(jobID2));
        LOG.info("jobID1 equals jobID3: {}", jobID1.equals(jobID3));

        LOG.info("jobID1 hashcode: {}", jobID1.hashCode());
        LOG.info("jobID2 hashcode: {}", jobID2.hashCode());
        LOG.info("jobID3 hashcode: {}", jobID3.hashCode());
    }

    public long getUpperPart() {
        return upperPart;
    }

    public long getLowerPart() {
        return lowerPart;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        checkState(o instanceof JobIDPOJO, "o is not JobIDPOJO");
        JobIDPOJO jobIDPOJO = (JobIDPOJO) o;
        return upperPart == jobIDPOJO.upperPart && lowerPart == jobIDPOJO.lowerPart;
    }

    @Override
    public int hashCode() {
        return Objects.hash(upperPart, lowerPart);
    }

    @Override
    public String toString() {
        return "JobIDPOJO{"
                + "upperPart=" + upperPart
                + ", lowerPart=" + lowerPart
                + '}';
    }
}