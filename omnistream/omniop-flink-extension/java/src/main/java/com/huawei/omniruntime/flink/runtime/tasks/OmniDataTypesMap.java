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

package com.huawei.omniruntime.flink.runtime.tasks;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OmniDataTypesMap {
    /*
     * Create an immutable map to prevent modification after initialization
     */
    public static final Map<String, Integer> DATA_TYPE_MAP = createDataTypeMap();

    private static Map<String, Integer> createDataTypeMap() {
        Map<String, Integer> map = new HashMap<>();
        map.put("CHAR", 0);
        map.put("VARCHAR", 1);
        map.put("BOOLEAN", 2);
        map.put("DECIMAL", 5);
        map.put("TINYINT", 6);
        map.put("SMALLINT", 7);
        map.put("INTEGER", 8);
        map.put("BIGINT", 9);
        map.put("INTERVAL_DAY", 9);
        map.put("FLOAT", 10);
        map.put("DOUBLE", 11);
        map.put("DATE", 12);
        map.put("TIMESTAMP", 14);
        map.put("ARRAY", 17);
        map.put("MAP", 19);
        map.put("ROW", 20);
        map.put("INVALID", 21);
        return Collections.unmodifiableMap(map);
    }
}
// todo: add more datatypes to the map
// RexNode sqlTypeName:
// BOOLEAN, INTEGER, VARCHAR, DATE, TIME, TIMESTAMP, NULL, DECIMAL,
// ANY, CHAR, BINARY, VARBINARY, TINYINT, SMALLINT, BIGINT, REAL,
// DOUBLE, SYMBOL, INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH,
// INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE,
// INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE,
// INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND,
// INTERVAL_SECOND, TIME_WITH_LOCAL_TIME_ZONE, TIME_TZ,
// TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIMESTAMP_TZ,
// FLOAT, MULTISET, DISTINCT, STRUCTURED, ROW, CURSOR, COLUMN_LIST
// OMNIDataTypes:
// OMNI_NONE = 0,
// OMNI_INT = 1,
// OMNI_LONG = 2,
// OMNI_DOUBLE = 3,
// OMNI_BOOLEAN = 4,
// OMNI_SHORT = 5,
// OMNI_DECIMAL64 = 6,
// OMNI_DECIMAL128 = 7,
// OMNI_DATE32 = 8,
// OMNI_DATE64 = 9,
// OMNI_TIME32 = 10,
// OMNI_TIME64 = 11,
// OMNI_TIMESTAMP = 12,
// OMNI_INTERVAL_MONTHS = 13,
// OMNI_INTERVAL_DAY_TIME = 14,
// OMNI_VARCHAR = 15,
// OMNI_CHAR = 16,
// OMNI_CONTAINER = 17,
// OMNI_INVALID
