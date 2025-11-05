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

package com.huawei.omniruntime.flink;

import static org.apache.flink.runtime.taskexecutor.TaskManagerRunner.FAILURE_EXIT_CODE;
import static org.apache.flink.runtime.taskexecutor.TaskManagerRunner.loadConfiguration;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * TNELLibrary
 *
 * @since 2025-04-27
 */
public class TNELLibrary {
    private static final Logger LOG = LoggerFactory.getLogger(TNELLibrary.class);

    private static final String LOAD_SO_KEY = "omni.taskmanager.binaryfiles";
    private static native void initialize();

    /**
     * loadLibrary
     * @param args args
     */
    public static void loadLibrary(String[] args) {
        Configuration configuration = null;

        try {
            configuration = loadConfiguration(args);
        } catch (FlinkParseException fpe) {
            LOG.error("Could not load the configuration.", fpe);
            System.exit(FAILURE_EXIT_CODE);
        }

        String[] filePaths = configuration.getString(LOAD_SO_KEY, "").split(",");
        for (String filePath : filePaths) {
            Path path = Paths.get(filePath);
            if (!path.isAbsolute()) {
                LOG.warn("the load file path is not an absolute path, please check your configuration");
                continue;
            }
            if (Files.notExists(path)) {
                LOG.warn("the load file not exists, please check your configuration");
                continue;
            }
            LOG.info("loading");
            try {
                System.load(path.toAbsolutePath().normalize().toString());
            } catch (Exception ex) {
                LOG.error("load so file {} failed, exception : {}", filePath, ex);
            }
        }
        System.loadLibrary("tnel");

        LOG.info("Loading Task Native Execution Library");
        initialize();
    }
}
