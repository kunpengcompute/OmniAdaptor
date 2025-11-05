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

package com.huawei.omniruntime.flink.streaming.runtime.task;

import com.huawei.omniruntime.flink.utils.JsonUtils;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;

import java.util.List;

public class StreamTaskUtils {
    public static long[] convertChannelInfo(List<InputChannelInfo> channelInfos) {
        return channelInfos.stream()
                .mapToLong( channelInfo ->
                        (long) channelInfo.getGateIdx() << 32 | channelInfo.getInputChannelIdx())
                .toArray();
    }

    public static String convertChannelInfoToJson (List<InputChannelInfo> channelInfos) {
        long[] channelInfo = convertChannelInfo(channelInfos);
        return JsonUtils.longToJsonString(channelInfo, "input_channels");
    }
}
