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

package com.huawei.omniruntime.flink.utils;

import org.apache.flink.api.common.JobID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

final public class UdfUtil {
    private static final Logger LOG = LoggerFactory.getLogger(UdfUtil.class);
    private static final String UDF_PATH = "native_dir/";

    private static final HashMap<JobID, String> JobList = new HashMap<>();

    public static void extractUDFSo(File jarFile, JobID jobId) {
        // 获取 JAR 文件的父目录（即同级目录）
        String destDir = jarFile.getParent();
        String jarPath = jarFile.getPath();

        try (JarFile jar = new JarFile(jarPath)) {
            Enumeration<JarEntry> entries = jar.entries();

            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String entryName = entry.getName();

                // 过滤非 udf_so 目录下的文件
                if (!entryName.startsWith(UDF_PATH)) {
                    continue;
                }
                if (!isSecurePath(entryName)) {
                    throw new IOException("path is not secure!");
                }
                File destFile = new File(destDir, entryName);
                if (entry.isDirectory()) {
                    // 创建目录
                    destFile.mkdirs();
                } else {
                    // 创建父目录（如果不存在）
                    File parentDir = destFile.getParentFile();
                    if (!parentDir.exists()) {
                        parentDir.mkdirs();
                    }
                    // 写入文件内容
                    try (InputStream is = jar.getInputStream(entry);
                         OutputStream os = Files.newOutputStream(destFile.toPath())) {
                        byte[] buffer = new byte[4096];
                        int bytesRead;
                        while ((bytesRead = is.read(buffer)) != -1) {
                            os.write(buffer, 0, bytesRead);
                        }
                    }
                }
            }
            JobList.put(jobId, destDir + "/" + UDF_PATH);
            LOG.info("udf_so is load success.");
        } catch (IOException e) {
            LOG.error("udf_so is load error.Exception:" + e.getMessage());
        }
    }

    public static String getJobJarPath(JobID jobId) {
        return JobList.getOrDefault(jobId, "");
    }

    /**
     * 校验是否为安全路径, 避免跨路径攻击
     *
     * @param path path
     * @return 是否为安全路径
     */
    private static boolean isSecurePath(String path) {
        if (path == null || path.isEmpty()) {
            return false;
        }
        if (path.contains("..") || path.startsWith("/") || path.startsWith("\\") || path.startsWith("$")) {
            return false;
        }
        return true;
    }
}
