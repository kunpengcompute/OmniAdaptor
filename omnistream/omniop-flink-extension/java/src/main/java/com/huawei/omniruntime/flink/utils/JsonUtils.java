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

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * JsonUtils
 *
 * @since 2025-04-27
 */
public class JsonUtils {
    /**
     * mergeJsonStringsTogether
     *
     * @param jsonStrings jsonStrings
     * @return String
     */
    public static String mergeJsonStringsTogether(String... jsonStrings) {
        JSONObject mergedJson = new JSONObject();

        for (String jsonString : jsonStrings) {
            JSONObject jsonObject = new JSONObject(jsonString);
            for (String key : jsonObject.keySet()) {
                mergedJson.put(key, jsonObject.get(key));
            }
        }

        return mergedJson.toString();
    }

    /**
     * longToJsonString
     *
     * @param inputChannels inputChannels
     * @param attributeName attributeName
     * @return String
     */
    public static String longToJsonString(long[] inputChannels, String attributeName) {
        // Create a JSON object
        JSONObject jsonObject = new JSONObject();

        // Convert the long array to a JSONArray
        JSONArray inputChannelsArray = new JSONArray();
        for (long channel : inputChannels) {
            inputChannelsArray.put(channel);
        }

        // Add the JSONArray as the value of "input_channels"
        jsonObject.put(attributeName, inputChannelsArray);
        return jsonObject.toString();
    }
}
