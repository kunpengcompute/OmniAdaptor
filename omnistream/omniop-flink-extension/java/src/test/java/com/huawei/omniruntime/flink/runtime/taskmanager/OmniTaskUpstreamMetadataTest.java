package com.huawei.omniruntime.flink.runtime.taskmanager;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OmniTaskUpstreamMetadataTest {
    @Test
    void preservesGateOrderAndEmptySourceInputs() throws Exception {
        assertEquals(Arrays.asList(true, false, true),
                OmniTask.getUpstreamNative(" [true, false, true] "));
        assertEquals(Collections.emptyList(), OmniTask.getUpstreamNative("[]"));
    }

    @Test
    void rejectsMissingMalformedOrUnknownFlags() {
        for (String value : new String[] {null, "", "true", "[yes]", "[true,]", "[,false]", "[TRUE]"}) {
            assertThrows(IOException.class, () -> OmniTask.getUpstreamNative(value));
        }
    }
}
