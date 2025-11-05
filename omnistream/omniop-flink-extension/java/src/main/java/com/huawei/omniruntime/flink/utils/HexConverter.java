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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HexConverter
 *
 * @since 2025-04-27
 */
public class HexConverter {
    private static final Logger LOG = LoggerFactory.getLogger(HexConverter.class);
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    /**
     * bytesToHex
     *
     * @param bytes bytes
     * @return String
     */
    public static String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        hexString.append("Hex: ");
        char[] hex2 = new char[2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hex2[0] = HEX_ARRAY[v >>> 4];
            hex2[1] = HEX_ARRAY[v & 0x0F];
            hexString.append(hex2);
            hexString.append(" ");
        }

        return hexString.toString();
    }

    public static void main(String[] args) {
        byte[] byteArray = {(byte) 0x0A, (byte) 0x0F, (byte) 0xFF};
        String hexString = bytesToHex(byteArray);
        LOG.info(hexString); // Output: 0A0FFF
    }
}