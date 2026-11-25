/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * OmniAdaptor is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

package com.huawei.omniruntime.flink.utils;

import org.apache.flink.table.types.logical.*;

/**
 * manage the system's configuration information
 *
 * @since 2025/04/22
 * @version 1.0.0
 */
public class DescriptionUtil {
    /**
     * getFieldType
     *
     * @param fieldType fieldType
     * @return String
     */
    public static String getFieldType(LogicalType fieldType) {
        LogicalTypeRoot typeRoot = fieldType.getTypeRoot();
        String typeName = typeRoot.toString();
        if (typeRoot == LogicalTypeRoot.VARCHAR) {
            if (fieldType instanceof VarCharType) {
                VarCharType varcharType = (VarCharType) fieldType;
                typeName += "(" + varcharType.getLength() + ")";
            }
        }
        if (typeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE
                || typeRoot == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            // Cast to TimestampType to access precision
            if (fieldType instanceof TimestampType) {
                int precision = ((TimestampType) fieldType).getPrecision();
                typeName += "(" + precision + ")";
            }
            if (fieldType instanceof LocalZonedTimestampType) {
                int precision = ((LocalZonedTimestampType) fieldType).getPrecision();
                typeName += "(" + precision + ")";
            }
        }
        if (typeRoot == LogicalTypeRoot.DECIMAL) {
            if (fieldType instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) fieldType;
                Integer precision = decimalType.getPrecision();
                Integer scale = decimalType.getScale();
                if (precision > 19) {
                    typeName += "128";
                } else {
                    typeName += "64";
                }
                typeName += "(" + precision + "," + scale + ")";
            }
        }
        return typeName;
    }
}
