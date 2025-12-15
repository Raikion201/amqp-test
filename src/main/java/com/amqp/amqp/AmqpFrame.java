package com.amqp.amqp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.util.ReferenceCounted;

/**
 * AMQP Frame that properly manages ByteBuf lifecycle through ByteBufHolder.
 * This ensures Netty automatically releases ByteBufs when frames are encoded/decoded.
 */
public class AmqpFrame implements ByteBufHolder {
    public static final int FRAME_HEADER_SIZE = 7;
    public static final int FRAME_END_SIZE = 1;
    public static final byte FRAME_END = (byte) 0xCE;

    private final byte type;
    private final short channel;
    private final int size;
    private final ByteBuf payload;

    public AmqpFrame(byte type, short channel, ByteBuf payload) {
        this.type = type;
        this.channel = channel;
        this.payload = payload;
        this.size = payload.readableBytes();
    }

    public byte getType() {
        return type;
    }

    public short getChannel() {
        return channel;
    }

    public int getSize() {
        return size;
    }

    public ByteBuf getPayload() {
        return payload;
    }

    // ByteBufHolder implementation for proper lifecycle management
    @Override
    public ByteBuf content() {
        return payload;
    }

    @Override
    public AmqpFrame copy() {
        return new AmqpFrame(type, channel, payload.copy());
    }

    @Override
    public AmqpFrame duplicate() {
        return new AmqpFrame(type, channel, payload.duplicate());
    }

    @Override
    public AmqpFrame retainedDuplicate() {
        return new AmqpFrame(type, channel, payload.retainedDuplicate());
    }

    @Override
    public AmqpFrame replace(ByteBuf content) {
        return new AmqpFrame(type, channel, content);
    }

    @Override
    public AmqpFrame retain() {
        payload.retain();
        return this;
    }

    @Override
    public AmqpFrame retain(int increment) {
        payload.retain(increment);
        return this;
    }

    @Override
    public AmqpFrame touch() {
        payload.touch();
        return this;
    }

    @Override
    public AmqpFrame touch(Object hint) {
        payload.touch(hint);
        return this;
    }

    @Override
    public int refCnt() {
        return payload.refCnt();
    }

    @Override
    public boolean release() {
        return payload.release();
    }

    @Override
    public boolean release(int decrement) {
        return payload.release(decrement);
    }
    
    public enum FrameType {
        METHOD(1),
        HEADER(2),
        BODY(3),
        HEARTBEAT(8);
        
        private final byte value;
        
        FrameType(int value) {
            this.value = (byte) value;
        }
        
        public byte getValue() {
            return value;
        }
        
        public static FrameType fromValue(byte value) {
            for (FrameType type : values()) {
                if (type.value == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown frame type: " + value);
        }
    }
}