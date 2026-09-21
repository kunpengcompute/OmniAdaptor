package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RemoteFallbackPartitionRequestTest {
    @Test
    void roundTripsConsumerEngineAndFrameLength() {
        EmbeddedChannel encoder = new EmbeddedChannel(new NettyMessage.NettyMessageEncoder());
        EmbeddedChannel decoder = new EmbeddedChannel(new NettyMessage.NettyMessageDecoder());
        try {
            // Alternate consumers on the same connection, including repeated requests.
            for (boolean nativeConsumer : new boolean[] {true, false, true, false}) {
                NettyMessage.PartitionRequest request = new NettyMessage.PartitionRequest(
                        new ResultPartitionID(), 3, new InputChannelID(), 7, nativeConsumer);
                encoder.writeOutbound(request);
                ByteBuf frame = encoder.readOutbound();
                assertEquals(frame.readableBytes(), frame.getInt(frame.readerIndex()));
                decoder.writeInbound(frame);
                NettyMessage.PartitionRequest decoded = decoder.readInbound();
                assertEquals(request.partitionId, decoded.partitionId);
                assertEquals(request.receiverId, decoded.receiverId);
                assertEquals(3, decoded.queueIndex);
                assertEquals(7, decoded.credit);
                assertEquals(nativeConsumer, decoded.isNative);
            }
        } finally {
            encoder.finishAndReleaseAll();
            decoder.finishAndReleaseAll();
        }
    }
}
