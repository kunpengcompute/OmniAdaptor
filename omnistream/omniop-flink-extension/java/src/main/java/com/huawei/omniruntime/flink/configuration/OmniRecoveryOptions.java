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

package com.huawei.omniruntime.flink.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

public class OmniRecoveryOptions {
    public static final String RECOVERY_SAVEPOINT_FORMAT_OMNI_INTERNAL = "OMNI_INTERNAL";
    public static final String RECOVERY_SAVEPOINT_FORMAT_FLINK_COMPATIBLE = "FLINK_COMPATIBLE";
    public static final String RECOVERY_SAVEPOINT_FORMAT_CONFIG_NAME = "omni.recovery.savepoint.format";

    public static final ConfigOption<String> RECOVERY_SAVEPOINT_FORMAT_OPTION = ConfigOptions.key(RECOVERY_SAVEPOINT_FORMAT_CONFIG_NAME)
                                .stringType()
                                .defaultValue("");

    private static final Map<String, String> RECOVERY_SAVEPOINT_FORMAT_CONFIG_MAP = new HashMap<String, String>(){{
        put("", RECOVERY_SAVEPOINT_FORMAT_OMNI_INTERNAL);
        put("compatible", RECOVERY_SAVEPOINT_FORMAT_FLINK_COMPATIBLE);
    }};

    public static String resolveRecoverySavepointFormat(ReadableConfig config) {
        String parameter = config.get(RECOVERY_SAVEPOINT_FORMAT_OPTION);

        String format = RECOVERY_SAVEPOINT_FORMAT_CONFIG_MAP.get(parameter);

        if (format == null) {
            throw new IllegalArgumentException("Invalid " + RECOVERY_SAVEPOINT_FORMAT_CONFIG_NAME + " \"" + parameter
                    + "\", " + RECOVERY_SAVEPOINT_FORMAT_CONFIG_NAME + " must be \"compatible\" or empty");
        }

        return format;
    }
}
