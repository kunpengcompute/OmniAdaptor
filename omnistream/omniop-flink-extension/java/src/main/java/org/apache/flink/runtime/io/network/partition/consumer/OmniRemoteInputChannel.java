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

package org.apache.flink.runtime.io.network.partition.consumer;

import com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor.ResultPartitionIDPOJO;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Optional;

/**
 * OmniRemoteInputChannel
 *
 * @since 2025-04-27
 */
public class OmniRemoteInputChannel {
    private static final Logger LOG = LoggerFactory.getLogger(OmniLocalInputChannel.class);
    private RemoteInputChannel remoteInputChannel;
    private boolean isConnected = false;
    private boolean dataReceived = false;
    private boolean fromOriginal = false;

    public OmniRemoteInputChannel(RemoteInputChannel remoteInputChannel) {
        this.remoteInputChannel = remoteInputChannel;
    }

    public RemoteInputChannel getRemoteInputChannel() {
        return remoteInputChannel;
    }

    public void setRemoteInputChannel(RemoteInputChannel remoteInputChannel) {
        this.remoteInputChannel = remoteInputChannel;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void setConnected(boolean connected) {
        isConnected = connected;
    }

    public void setFromOriginal(boolean fromOriginal) {
        this.fromOriginal = fromOriginal;
    }

    public boolean getFromOriginal() {
        return fromOriginal;
    }

    /**
     * getChannelIndex
     *
     * @return int
     */
    public int getChannelIndex() {
        return remoteInputChannel.getChannelIndex();
    }

    /**
     * getGateIndex
     *
     * @return int
     */
    public int getGateIndex() {
        return remoteInputChannel.getChannelInfo().getGateIdx();
    }

    /**
     * getNextBuffer
     *
     * @return Optional<InputChannel.BufferAndAvailability>
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     * @throws IOException IOException
     */
    public Optional<InputChannel.BufferAndAvailability> getNextBuffer()
            throws IOException, InterruptedException, IOException {
        return remoteInputChannel.getNextBuffer();
    }

    /**
     * getSingleInputGateConsumedSubpartitionIndex
     *
     * @return int
     */
    public int getSingleInputGateConsumedSubpartitionIndex() {
        Class clazz = SingleInputGate.class;
        try {
            Field field = clazz.getDeclaredField("consumedSubpartitionIndex");
            field.setAccessible(true);
            return field.getInt(this.remoteInputChannel.inputGate);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public void setDataReceived(boolean dataReceived) {
        this.dataReceived = dataReceived;
    }
    public boolean isDataReceived() {
        return dataReceived;
    }
    public void resumeConsumption() throws IOException {
        remoteInputChannel.resumeConsumption();
    }

    public boolean addExtraInfoToNativeRemoteInputChannel(long nativeTaskRef) {
        ResultPartitionIDPOJO resultPartitionIDPOJO = new ResultPartitionIDPOJO(remoteInputChannel.getPartitionId());
        JSONObject jsonObject = new JSONObject(resultPartitionIDPOJO);
        String parititonIdString = jsonObject.toString();
        long nativeRemoteInputChannelRef = doAddExtraInfoToNativeRemoteInputChannel(nativeTaskRef, parititonIdString);
        if(nativeRemoteInputChannelRef != -1) {
            LOG.info("Successfully add extra info to native remote input channel with " +
                            "channelIndex: {} nativeRemoteInputChannelRef = {}", remoteInputChannel.getChannelIndex(),
                    nativeRemoteInputChannelRef);
            return true;
        } else {
            LOG.error("Failed to add extra info to native remote input channel with channelIndex: {} ", remoteInputChannel.getChannelIndex());
            return false;
        }
    }

    public native long doAddExtraInfoToNativeRemoteInputChannel(long nativeTaskRef, String partitionIdString);
}
