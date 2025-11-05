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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BinaryRowDataUtils
 *
 * @since 2025-04-27
 */
public class BinaryRowDataUtils {
    /**
     * LOG
     */
    private static final Logger LOG = LoggerFactory.getLogger(BinaryRowDataUtils.class);

    /**
     * hexContentBinaryRowData
     *
     * @param binaryRowData binaryRowData
     * @return String
     */
    public static String hexContentBinaryRowData(BinaryRowData binaryRowData) {
        MemorySegment[] segments = binaryRowData.getSegments();
        int offset = binaryRowData.getOffset();
        int sizInBytes = binaryRowData.getSizeInBytes();

        // assume there is only one segment
        MemorySegment segment = segments[0];
        int size = segment.size();
        int printSize = offset + sizInBytes > size ? size - offset : sizInBytes;
        byte[] bytes = new byte[printSize];
        segment.get(offset, bytes, 0, printSize);

        return HexConverter.bytesToHex(bytes);
    }
}
