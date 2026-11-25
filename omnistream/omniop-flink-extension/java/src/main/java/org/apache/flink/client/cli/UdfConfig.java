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

package org.apache.flink.client.cli;

import com.huawei.omniruntime.flink.client.NativeMain;

import java.util.Properties;

public class UdfConfig {

    private static final UdfConfig INSTANCE = new UdfConfig();
    private Properties config = new Properties();
    private boolean isAI4C = false;

    public static UdfConfig getINSTANCE() {
        return INSTANCE;
    }

    public void setJarNumber(String jarFilePath) {
        config = NativeMain.getConfig(jarFilePath);
        for (String stringPropertyName : config.stringPropertyNames()) {
            if (stringPropertyName.equals("isAI4C")) {
                isAI4C = true;
            }

            if (!config.getProperty(stringPropertyName).endsWith(".so")) {
                config.remove(stringPropertyName);
            }
        }
    }

    public Properties getConfig() {
        return config;
    }

    public boolean isAI4C() {
        return isAI4C;
    }
}
