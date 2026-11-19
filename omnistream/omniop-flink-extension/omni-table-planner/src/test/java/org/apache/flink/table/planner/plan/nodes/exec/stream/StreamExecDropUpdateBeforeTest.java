/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Unit tests for the shadowed {@link StreamExecDropUpdateBefore} operator description. */
public class StreamExecDropUpdateBeforeTest {

    @Test
    public void testExtraDescriptionContainsMarkerAndTypeLists() throws Exception {
        RowType rowType =
                RowType.of(
                        new LogicalType[] {new BigIntType(), new VarCharType(20)},
                        new String[] {"id", "name"});
        JsonNode description = getExtraDescription(rowType, "DropUpdateBefore");

        assertEquals("DropUpdateBefore", description.get("operator").asText());
        assertEquals("DropUpdateBefore", description.get("originDescription").asText());

        List<String> expected = Arrays.asList("BIGINT", "VARCHAR(20)");
        assertEquals(expected, readTypeList(description.get("inputTypes")));
        assertEquals(expected, readTypeList(description.get("outputTypes")));
        assertEquals(
                readTypeList(description.get("inputTypes")),
                readTypeList(description.get("outputTypes")));
    }

    @Test
    public void testExtraDescriptionNormalizesParameterizedAndNotNullTypes() throws Exception {
        RowType rowType =
                RowType.of(
                        new LogicalType[] {
                            new BigIntType(false), new TimestampType(3), new DecimalType(20, 2)
                        },
                        new String[] {"id", "ts", "amount"});
        JsonNode description = getExtraDescription(rowType, "DropUpdateBefore[3]");

        assertEquals("DropUpdateBefore[3]", description.get("originDescription").asText());

        List<String> expected =
                Arrays.asList(
                        "BIGINT NOT NULL",
                        "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                        "DECIMAL128(20,2)");
        assertEquals(expected, readTypeList(description.get("inputTypes")));
        assertEquals(expected, readTypeList(description.get("outputTypes")));
    }

    private static JsonNode getExtraDescription(RowType rowType, String originDescription)
            throws Exception {
        StreamExecDropUpdateBefore node =
                new StreamExecDropUpdateBefore(
                        new Configuration(), InputProperty.DEFAULT, rowType, originDescription);
        Method method = StreamExecDropUpdateBefore.class.getDeclaredMethod("getExtraDescription");
        method.setAccessible(true);
        String json = (String) method.invoke(node);
        assertNotNull(json);
        ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        return objectMapper.readTree(json);
    }

    private static List<String> readTypeList(JsonNode node) {
        List<String> types = new ArrayList<>();
        node.forEach(field -> types.add(field.asText()));
        return types;
    }
}
