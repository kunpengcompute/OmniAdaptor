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

import static org.apache.flink.util.Preconditions.checkState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * KeyFieldInfoPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class KeyFieldInfoPOJO {
    private static final Logger LOG = LoggerFactory.getLogger(KeyFieldInfoPOJO.class);
    private String fieldName;
    private String fieldTypeName;
    private int fieldIndex;

    // No-argument constructor (required for some frameworks like Jackson)
    public KeyFieldInfoPOJO() {
    }

    // Constructor with all fields
    public KeyFieldInfoPOJO(String fieldName, String fieldTypeName, int fieldIndex) {
        this.fieldName = fieldName;
        this.fieldTypeName = fieldTypeName;
        this.fieldIndex = fieldIndex;
    }

    public static void main(String[] args) {
        Map<String, Object> fieldInfo = new HashMap<>();
        fieldInfo.put("fieldName", "exampleField");
        fieldInfo.put("fieldTypeName", "java.lang.String");
        fieldInfo.put("fieldIndex", 0);
        checkState(fieldInfo.get("fieldName") instanceof String, "fieldInfo.get(\"fieldName\") is not String");
        checkState(fieldInfo.get("fieldTypeName") instanceof String, "fieldInfo.get(\"fieldTypeName\") is not String");
        String fieldName = (String) fieldInfo.get("fieldName");
        String fieldTypeName = (String) fieldInfo.get("fieldTypeName");
        int fieldIndex = (int) fieldInfo.get("fieldIndex");

        KeyFieldInfoPOJO myField = new KeyFieldInfoPOJO(fieldName, fieldTypeName, fieldIndex);

        LOG.info("Field name: {}", myField.getFieldName()); // Output: exampleField
        LOG.info("Field type name: {}", myField.getFieldTypeName()); // Output: java.lang.String
        LOG.info("Field index: {}", myField.getFieldIndex()); // Output: 0

        LOG.info("Field info: {}", myField); // Output: FieldInfo{fieldName='exampleField', fieldTypeName='java.lang.String', fieldIndex=0}
    }

    // Getters
    public String getFieldName() {
        return fieldName;
    }

    // Setters
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldTypeName() {
        return fieldTypeName;
    }

    public void setFieldTypeName(String fieldTypeName) {
        this.fieldTypeName = fieldTypeName;
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    public void setFieldIndex(int fieldIndex) {
        this.fieldIndex = fieldIndex;
    }

    @Override
    public String toString() {
        return "FieldInfo{"
                + "fieldName='" + fieldName + '\''
                + ", fieldTypeName='" + fieldTypeName + '\''
                + ", fieldIndex=" + fieldIndex
                + '}';
    }
}