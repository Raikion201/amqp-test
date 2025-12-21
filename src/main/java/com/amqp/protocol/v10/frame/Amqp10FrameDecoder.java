package com.amqp.protocol.v10.frame;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Netty decoder for AMQP 1.0 frames.
 *
 * Decodes the wire format into Amqp10Frame objects.
 */
public class Amqp10FrameDecoder extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(Amqp10FrameDecoder.class);

    private static final int MAX_FRAME_SIZE = 1024 * 1024; // 1MB default max

    private final int maxFrameSize;

    public Amqp10FrameDecoder() {
        this(MAX_FRAME_SIZE);
    }

    public Amqp10FrameDecoder(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Need at least 4 bytes for frame size
        if (in.readableBytes() < 4) {
            return;
        }

        in.markReaderIndex();

        // Read frame size (includes the 4 size bytes)
        int frameSize = in.readInt();

        // Validate frame size
        if (frameSize < Amqp10Frame.HEADER_SIZE) {
            throw new IllegalStateException("Frame size too small: " + frameSize);
        }
        if (frameSize > maxFrameSize) {
            throw new IllegalStateException("Frame size exceeds maximum: " + frameSize + " > " + maxFrameSize);
        }

        // Check if we have the complete frame
        int remainingFrameBytes = frameSize - 4; // Subtract the size field we already read
        if (in.readableBytes() < remainingFrameBytes) {
            in.resetReaderIndex();
            return;
        }

        // Read header fields
        byte doff = in.readByte();
        byte typeCode = in.readByte();
        int channel = in.readUnsignedShort();

        // Validate doff
        if (doff < Amqp10Frame.MIN_DOFF) {
            throw new IllegalStateException("Invalid DOFF: " + doff);
        }

        // Skip extended header if present (doff > 2)
        int extendedHeaderSize = (doff - Amqp10Frame.MIN_DOFF) * 4;
        if (extendedHeaderSize > 0) {
            in.skipBytes(extendedHeaderSize);
        }

        // Calculate body size
        int headerBytesRead = 4 + extendedHeaderSize; // doff, type, channel, extended
        int bodySize = frameSize - Amqp10Frame.HEADER_SIZE - extendedHeaderSize;

        // Read body
        ByteBuf body = null;
        if (bodySize > 0) {
            body = in.readRetainedSlice(bodySize);
        }

        FrameType type = FrameType.fromCode(typeCode);
        Amqp10Frame frame = new Amqp10Frame(frameSize, doff, type, channel, body);

        logger.debug("Decoded AMQP 1.0 frame: {}", frame);
        out.add(frame);
    }
}
