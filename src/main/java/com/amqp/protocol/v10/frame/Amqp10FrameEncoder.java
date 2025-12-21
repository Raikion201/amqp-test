package com.amqp.protocol.v10.frame;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty encoder for AMQP 1.0 frames.
 *
 * Encodes Amqp10Frame objects to wire format.
 */
public class Amqp10FrameEncoder extends MessageToByteEncoder<Amqp10Frame> {

    private static final Logger logger = LoggerFactory.getLogger(Amqp10FrameEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, Amqp10Frame frame, ByteBuf out) throws Exception {
        frame.encode(out);
        logger.debug("Encoded AMQP 1.0 frame: {}", frame);
    }
}
