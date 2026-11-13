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

package com.huawei.omniruntime.flink.runtime.api.graph.json.operatorchain.tablescan;

import java.util.List;
import java.util.Properties;

/**
 * KafkaFormatPOJO
 *
 * @since 2025/4/8
 */
public class KafkaFormatPojo {
    private boolean batch;

    private String format;

    private boolean emitProgressiveWatermarks;

    private Properties properties;

    private String watermarkStrategy;

    private long outOfOrdernessMillis;

    private long rowtimeFieldIndex = -1;

    private String deserializationSchema;

    private boolean hasMetadata;

    private List<String> outputNames;

    private List<String> outputTypes;

    private Boolean upsertMode;

    private List<Integer> keyProjection;

    private List<Integer> valueProjection;

    private List<String> keyFieldNames;

    private List<String> valueFieldNames;

    public boolean isBatch() {
        return batch;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public boolean isEmitProgressiveWatermarks() {
        return emitProgressiveWatermarks;
    }

    public void setEmitProgressiveWatermarks(boolean emitProgressiveWatermarks) {
        this.emitProgressiveWatermarks = emitProgressiveWatermarks;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String getWatermarkStrategy() {
        return watermarkStrategy;
    }

    public void setWatermarkStrategy(String watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    public long getOutOfOrdernessMillis() {
        return outOfOrdernessMillis;
    }

    public void setOutOfOrdernessMillis(long outOfOrdernessMillis) {
        this.outOfOrdernessMillis = outOfOrdernessMillis;
    }

    public long getRowtimeFieldIndex() {
        return rowtimeFieldIndex;
    }

    public void setRowtimeFieldIndex(long value) {
        this.rowtimeFieldIndex = value;
    }

    public String getDeserializationSchema() {
        return deserializationSchema;
    }

    public void setDeserializationSchema(String deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    public boolean isHasMetadata() {
        return hasMetadata;
    }

    public void setHasMetadata(boolean hasMetadata) {
        this.hasMetadata = hasMetadata;
    }

    public List<String> getOutputNames() {
        return outputNames;
    }

    public void setOutputNames(List<String> outputNames) {
        this.outputNames = outputNames;
    }

    public List<String> getOutputTypes() {
        return outputTypes;
    }

    public void setOutputTypes(List<String> outputTypes) {
        this.outputTypes = outputTypes;
    }

    public Boolean getUpsertMode() {
        return upsertMode;
    }

    public void setUpsertMode(Boolean upsertMode) {
        this.upsertMode = upsertMode;
    }

    public List<Integer> getKeyProjection() {
        return keyProjection;
    }

    public void setKeyProjection(List<Integer> keyProjection) {
        this.keyProjection = keyProjection;
    }

    public List<Integer> getValueProjection() {
        return valueProjection;
    }

    public void setValueProjection(List<Integer> valueProjection) {
        this.valueProjection = valueProjection;
    }

    public List<String> getKeyFieldNames() {
        return keyFieldNames;
    }

    public void setKeyFieldNames(List<String> keyFieldNames) {
        this.keyFieldNames = keyFieldNames;
    }

    public List<String> getValueFieldNames() {
        return valueFieldNames;
    }

    public void setValueFieldNames(List<String> valueFieldNames) {
        this.valueFieldNames = valueFieldNames;
    }
}
