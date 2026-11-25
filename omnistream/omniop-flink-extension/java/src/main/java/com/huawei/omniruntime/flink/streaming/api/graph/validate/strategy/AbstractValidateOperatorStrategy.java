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

package com.huawei.omniruntime.flink.streaming.api.graph.validate.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractValidateOperatorStrategy {

    public static final Map<String, Integer> RexTypeToIdMap = new HashMap<>();
    // 所有 validate 策略共用的类型白名单。OVERLAPS 自身仍在 RexNodeUtil 中拒绝精度大于 3
    // 的 TIME/TIMESTAMP；这里保留基线已有的 TIMESTAMP(9)，避免改变其他表达式的支持范围。
    // INT 与 INTEGER 等价：planner 侧 description 的类型字符串两种写法都可能出现
    // （Table API flatAggregate 的输出类型字符串化为 INT），白名单需要两者都认。
    protected static final Set<String> SUPPORT_DATA_TYPE = new HashSet<>(Arrays.asList(
            "BIGINT",
            "INT",
            "INTEGER",
            "TIME_WITHOUT_TIME_ZONE(0)",
            "TIME_WITHOUT_TIME_ZONE(1)",
            "TIME_WITHOUT_TIME_ZONE(2)",
            "TIME_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(0)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(1)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(2)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
            "TIMESTAMP_WITHOUT_TIME_ZONE(9)",
            "VARCHAR",
            "VARCHAR(2147483647)",
            "VARCHAR(2000)",
            "VARCHAR(9)",
            "STRING",
            "CHAR",
            "BOOLEAN",
            "DECIMAL64",
            "DECIMAL128",
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)"));
    private static final Logger LOG = LoggerFactory.getLogger(AbstractValidateOperatorStrategy.class);

    static {
        // This map converts Calcite RexNode SqlTypeName to OmniType Id
        RexTypeToIdMap.put("INT", 1);
        RexTypeToIdMap.put("INTEGER", 1);
        RexTypeToIdMap.put("BIGINT", 2);
        RexTypeToIdMap.put("DOUBLE", 3);
        RexTypeToIdMap.put("BOOLEAN", 4);
        RexTypeToIdMap.put("TINYINT", 5);
        RexTypeToIdMap.put("SMALLINT", 5);
        RexTypeToIdMap.put("DECIMAL", 6);
        RexTypeToIdMap.put("DECIMAL128", 7);
        RexTypeToIdMap.put("DATE", 8);
        RexTypeToIdMap.put("TIME", 10);
        RexTypeToIdMap.put("TIMESTAMP", 12);
        RexTypeToIdMap.put("INTERVAL_MONTH", 13);
        RexTypeToIdMap.put("INTERVAL_DAY", 14);
        RexTypeToIdMap.put("INTERVAL_SECOND", 14);
        RexTypeToIdMap.put("VARCHAR", 15);
        RexTypeToIdMap.put("CHAR", 16);
        RexTypeToIdMap.put("ROW", 17);
        RexTypeToIdMap.put("INVALID", 18);
        RexTypeToIdMap.put("TIME_WITHOUT_TIME_ZONE", 19); // TODO: Is this the same as TIME?
        RexTypeToIdMap.put("TIMESTAMP_WITHOUT_TIME_ZONE", 20); // TODO: Omni's TIMESTAMP uses int64_t, Flink has the possibility of accuracy>3
        RexTypeToIdMap.put("TIMESTAMP_TZ", 21); // TIMESTAMP_WITH_TIMEZONE
        RexTypeToIdMap.put("TIMESTAMP_WITH_LOCAL_TIME_ZONE", 24);
        RexTypeToIdMap.put("ARRAY", 23);
        RexTypeToIdMap.put("MULTISET", 24);
        RexTypeToIdMap.put("MAP", 25);
    }

    public abstract boolean executeValidateOperator(Map<String, Object> operatorInfoMap);

    public boolean validateDataTypes(List<List<String>> dataTypesList) {
        return dataTypesList.stream()
                // match DECIMAL64 and DECIMAL128
                .flatMap(List::stream)
                .allMatch(type -> {
                    String originalType = type;
                    type = stripNotNull(type);

                    if (type.matches("^VARCHAR\\([^)]*\\)$")) {
                        LOG.info("Normalized VARCHAR: '{}' -> 'VARCHAR'", type);
                        type = "VARCHAR";
                    }

                    if (type.matches("^DECIMAL64\\([^)]*\\)$")) {
                        LOG.info("Normalized DECIMAL64: '{}' -> 'DECIMAL64'", type);
                        type = "DECIMAL64";
                    }

                    if (type.matches("^DECIMAL128\\([^)]*\\)$")) {
                        LOG.info("Normalized DECIMAL128: '{}' -> 'DECIMAL128'", type);
                        type = "DECIMAL128";
                    }

                    if (!SUPPORT_DATA_TYPE.contains(type)) {
                        LOG.info("Unsupported data type: '{}' (original: '{}')", type, originalType);
                        return false;
                    }
                    return true;
                });
    }

    @SuppressWarnings("unchecked")
    public List<List<String>> getDataTypes(Map<String, Object> jsonMap, String... names) {
        List<List<String>> dataTypes = new ArrayList<>();
        for (String name : names) {
            dataTypes.add((List<String>) jsonMap.get(name));
        }
        return dataTypes;
    }

    protected String getStringInfo(Map<String, Object> jsonMap, String name) {
        Object info = jsonMap.get(name);
        if (info instanceof String) {
            return (String) info;
        } else {
            return null;
        }
    }

    /**
     * 剥离类型字符串末尾的 NOT NULL 后缀，兼容 DescriptionUtil.getFieldType() 追加的 nullable 信息。
     */
    public static String stripNotNull(String type) {
        if (type != null && type.endsWith(" NOT NULL")) {
            String stripped = type.substring(0, type.length() - " NOT NULL".length());
            LOG.info("stripNotNull: '{}' -> '{}'", type, stripped);
            return stripped;
        }
        return type;
    }
}
