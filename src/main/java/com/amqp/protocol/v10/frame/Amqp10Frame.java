package com.amqp.protocol.v10.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.Unpooled;

/**
 * AMQP 1.0 Frame structure.
 *
 * Frame format:
 * +-------+-------+-------+-------+-------+...+-------+-------+...+-------+
 * | SIZE (4 bytes)| DOFF  | TYPE  |CHANNEL| EXTENDED| BODY                |
 * +-------+-------+-------+-------+-------+...+-------+-------+...+-------+
 *
 * SIZE: Total frame size including the 4 size bytes
 * DOFF: Data offset in 4-byte words (minimum 2 for 8-byte header)
 * TYPE: Frame type (0x00 = AMQP, 0x01 = SASL)
 * CHANNEL: Channel number (for AMQP frames) or 0 (for SASL frames)
 * EXTENDED: Optional extended header (if DOFF > 2)
 * BODY: Frame body (performative + optional payload)
 */
public class Amqp10Frame implements ByteBufHolder {

    public static final int HEADER_SIZE = 8;
    public static final int MIN_DOFF = 2;

    private final int size;
    private final byte doff;
    private final FrameType type;
    private final int channel;
    private final ByteBuf body;

    public Amqp10Frame(FrameType type, int channel, ByteBuf body) {
        this.type = type;
        this.channel = channel;
        this.body = body;
        this.doff = MIN_DOFF;
        this.size = HEADER_SIZE + (body != null ? body.readableBytes() : 0);
    }

    public Amqp10Frame(int size, byte doff, FrameType type, int channel, ByteBuf body) {
        this.size = size;
        this.doff = doff;
        this.type = type;
        this.channel = channel;
        this.body = body;
    }

    public int getSize() {
        return size;
    }

    public byte getDoff() {
        return doff;
    }

    public FrameType getType() {
        return type;
    }

    public int getChannel() {
        return channel;
    }

    public ByteBuf getBody() {
        return body;
    }

    /**
     * Check if this is an AMQP frame (transport layer).
     */
    public boolean isAmqpFrame() {
        return type == FrameType.AMQP;
    }

    /**
     * Check if this is a SASL frame (security layer).
     */
    public boolean isSaslFrame() {
        return type == FrameType.SASL;
    }

    /**
     * Encode this frame to a ByteBuf.
     */
    public void encode(ByteBuf buffer) {
        int bodySize = body != null ? body.readableBytes() : 0;
        int frameSize = HEADER_SIZE + bodySize;

        buffer.writeInt(frameSize);
        buffer.writeByte(doff);
        buffer.writeByte(type.getCode());
        buffer.writeShort(channel);

        if (body != null && body.isReadable()) {
            buffer.writeBytes(body, body.readerIndex(), body.readableBytes());
        }
    }

    /**
     * Create a new AMQP frame.
     */
    public static Amqp10Frame createAmqpFrame(int channel, ByteBuf body) {
        return new Amqp10Frame(FrameType.AMQP, channel, body);
    }

    /**
     * Create a new SASL frame.
     */
    public static Amqp10Frame createSaslFrame(ByteBuf body) {
        return new Amqp10Frame(FrameType.SASL, 0, body);
    }

    /**
     * Create an empty AMQP frame (heartbeat).
     */
    public static Amqp10Frame createHeartbeat() {
        return new Amqp10Frame(FrameType.AMQP, 0, Unpooled.EMPTY_BUFFER);
    }

    @Override
    public ByteBuf content() {
        return body;
    }

    @Override
    public Amqp10Frame copy() {
        return new Amqp10Frame(size, doff, type, channel, body != null ? body.copy() : null);
    }

    @Override
    public Amqp10Frame duplicate() {
        return new Amqp10Frame(size, doff, type, channel, body != null ? body.duplicate() : null);
    }

    @Override
    public Amqp10Frame retainedDuplicate() {
        return new Amqp10Frame(size, doff, type, channel, body != null ? body.retainedDuplicate() : null);
    }

    @Override
    public Amqp10Frame replace(ByteBuf content) {
        return new Amqp10Frame(size, doff, type, channel, content);
    }

    @Override
    public int refCnt() {
        return body != null ? body.refCnt() : 1;
    }

    @Override
    public Amqp10Frame retain() {
        if (body != null) {
            body.retain();
        }
        return this;
    }

    @Override
    public Amqp10Frame retain(int increment) {
        if (body != null) {
            body.retain(increment);
        }
        return this;
    }

    @Override
    public Amqp10Frame touch() {
        if (body != null) {
            body.touch();
        }
        return this;
    }

    @Override
    public Amqp10Frame touch(Object hint) {
        if (body != null) {
            body.touch(hint);
        }
        return this;
    }

    @Override
    public boolean release() {
        return body == null || body.release();
    }

    @Override
    public boolean release(int decrement) {
        return body == null || body.release(decrement);
    }

    @Override
    public String toString() {
        return String.format("Amqp10Frame{size=%d, doff=%d, type=%s, channel=%d, bodySize=%d}",
                size, doff, type, channel, body != null ? body.readableBytes() : 0);
    }
}
