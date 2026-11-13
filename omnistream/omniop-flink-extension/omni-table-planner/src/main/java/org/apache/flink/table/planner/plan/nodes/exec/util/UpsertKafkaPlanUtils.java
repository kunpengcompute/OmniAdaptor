/* Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Mulan PSL v2. */
package org.apache.flink.table.planner.plan.nodes.exec.util;

import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.plan.utils.ReflectionUtils.retrievePrivateField;

/** Planner-local extraction of native upsert Kafka descriptors. */
public final class UpsertKafkaPlanUtils {
    private UpsertKafkaPlanUtils() {}

    public static boolean isUpsertKafkaSink(Object sink) {
        return "KafkaDynamicSink".equals(sink.getClass().getSimpleName()) && isUpsertMode(sink);
    }

    public static boolean isUpsertMode(Object connector) {
        return Boolean.TRUE.equals(retrievePrivateField(connector, "upsertMode"));
    }

    public static void populateSourceDescriptor(
            Map<String, Object> target, Object dynamic, Object keyDecoder, Object valueDecoder) {
        Object projectionCollector = retrievePrivateField(dynamic, "outputCollector");
        target.put("keyProjection", retrievePrivateField(projectionCollector, "keyProjection"));
        target.put("valueProjection", retrievePrivateField(projectionCollector, "valueProjection"));
        target.put("keyFieldNames", getDecoderFieldNames(keyDecoder));
        target.put("valueFieldNames", getDecoderFieldNames(valueDecoder));
    }

    public static boolean isSupportedJsonDecoder(Object decoder) {
        if (decoder == null || !"JsonRowDataDeserializationSchema".equals(decoder.getClass().getSimpleName())) {
            return false;
        }
        Object timestampFormat = retrievePrivateField(decoder, "timestampFormat");
        return "SQL".equals(String.valueOf(timestampFormat))
                && Boolean.FALSE.equals(retrievePrivateField(decoder, "failOnMissingField"))
                && Boolean.FALSE.equals(retrievePrivateField(decoder, "ignoreParseErrors"));
    }

    private static List<String> getDecoderFieldNames(Object decoder) {
        InternalTypeInfo<?> type = retrievePrivateField(decoder, "resultTypeInfo");
        return type.toRowType().getFieldNames();
    }

    public static void populateSinkProjection(Map<String, Object> target, Object tableSink) {
        target.put("upsertMode", true);
        DataType physicalType = retrievePrivateField(tableSink, "physicalDataType");
        target.put("physicalTypes",
                DescriptionUtil.getFieldTypeList(((RowType) physicalType.getLogicalType()).getFields()));
        target.put("keyProjection", retrievePrivateField(tableSink, "keyProjection"));
        target.put("valueProjection", retrievePrivateField(tableSink, "valueProjection"));
    }
}
