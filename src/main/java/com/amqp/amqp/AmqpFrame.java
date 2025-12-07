package com.amqp.amqp;

import io.netty.buffer.ByteBuf;

public class AmqpFrame {
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