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

package com.huawei.omniruntime.flink.client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Objects;


public class AI4C {
    private static final Logger LOG = LoggerFactory.getLogger(AI4C.class);
    private static HashMap<String, String> ai4cmap = new LinkedHashMap<>();
    public static String OPTIMIZE_FUNC_NAME = "eccf3286$2";
    static {
        ai4cmap.put("com_huawei_qinwei_JDFlinkWordCount_lambda_main_8119522c_1_org_apache_flink_api_common_functions_ReduceFunction.cpp", "59ef0750e9848888512bd32c707c0ae13af9ead976ab6bb794769f73e176425b");
        ai4cmap.put("com_huawei_qinwei_JDFlinkWordCount_lambda_main_8119522c_1_org_apache_flink_api_common_functions_ReduceFunction.h", "807f44405a2acc5dcf889afb3452a72f253a0560f39584e2cb5291516d491e98");
        ai4cmap.put("com_huawei_qinwei_JDFlinkWordCount_lambda_main_b9058687_1_org_apache_flink_api_java_functions_KeySelector.cpp", "b7c657bfc37472dd84db7d455d14b69e9c898852edeb64010a105075ad843ac0");
        ai4cmap.put("com_huawei_qinwei_JDFlinkWordCount_lambda_main_b9058687_1_org_apache_flink_api_java_functions_KeySelector.h", "e8681d3b117707c9cdfae5fc3421c8e7dbe03af770613e924e1465288a9770e5");
        ai4cmap.put("com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_1_org_apache_flink_api_common_functions_MapFunction.cpp", "7509836ef1c5957a2973d0e740bcbd048f232ec9b82e44f667b5fc2f4a0852d5");
        ai4cmap.put("com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_1_org_apache_flink_api_common_functions_MapFunction.h", "c3ef33f04acc30d5d1016c07a52cbfd9a807e2e9b1f803e482ccde5c48cbf49d");
        ai4cmap.put("com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_2_org_apache_flink_api_common_functions_MapFunction.cpp", "3b80fa4951247727683c895004aa0026c7742c356bf574434b8a05cc98f1146c");
        ai4cmap.put("com_huawei_qinwei_JDFlinkWordCount_lambda_main_eccf3286_2_org_apache_flink_api_common_functions_MapFunction.h", "29de251f95b1e4ad97929db2cd824c013fddafbd68bc4c4a919e923ddf770ece");
    }

    public static int activeAi4c(String cpp_path) {
        File directory = new File(cpp_path);
        if (directory.exists() && directory.isDirectory()) {
            if (!moveCpp(cpp_path)) {
                return -1;
            }
            return NativeMain.execute("bash " + cpp_path + "/ai4c.sh");
        } else {
            return -1;
        }
    }

    private static boolean moveCpp(String cpp_path) {
        Path workDir = Paths.get(cpp_path);
        String rootDir = "/ai4c/";
        String ai4c_cpp = rootDir + "ai4c-cpp/";
        try {
            // cpp
            for (String res : ai4cmap.keySet()) {
                extractToDirectory(ai4c_cpp + res, workDir);
            }
            // sh
            Path shFile = extractToDirectory(rootDir + "ai4c.sh", workDir);
            shFile.toFile().setExecutable(true);
            return true;
        } catch (IOException exception) {
            LOG.error("Move file failed.");
            return false;
        }
    }

    /**
     * 将 JAR 中的资源提取到指定的外部目录
     *
     * @param resourcePath JAR 内的路径 (例如 "/native_libs/libworker.cpp")
     * @param targetDir    外部存放的目录路径
     */
    public static Path extractToDirectory(String resourcePath, Path targetDir) throws IOException {
        // 从路径中解析出文件名 (例如 "libworker.cpp")
        String fileName = resourcePath.substring(resourcePath.lastIndexOf("/") + 1);
        Path targetFile = targetDir.resolve(fileName);

        try (InputStream in = AI4C.class.getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalArgumentException("File not found: " + resourcePath);
            }
            Files.copy(in, targetFile, StandardCopyOption.REPLACE_EXISTING);
        }
        return targetFile;
    }

    public static boolean isAi4c(String cpp_path) {
        try {
            for (Map.Entry<String, String> stringStringEntry : ai4cmap.entrySet()) {
                String hashCode = NativeMain.getFileHashCode(cpp_path + "/" + stringStringEntry.getKey());
                if (!Objects.equals(stringStringEntry.getValue(), hashCode)) {
                    LOG.info("Job is not AI4C! key: " + stringStringEntry.getKey() + " value: " + hashCode);
                    return false;
                }
            }
            LOG.info("Job is AI4C!");
            return true;
        } catch (Exception exception) {
            LOG.info("AI4C file not exist!");
            return false;
        }
    }
}
