package com.amqp.amqp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.List;

public class AmqpCodec {
    
    public static class AmqpFrameDecoder extends ByteToMessageDecoder {
        private static final int MIN_FRAME_SIZE = 8;
        
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            if (in.readableBytes() < MIN_FRAME_SIZE) {
                return;
            }
            
            int readerIndex = in.readerIndex();
            
            byte type = in.readByte();
            short channel = in.readShort();
            int size = in.readInt();
            
            if (in.readableBytes() < size + 1) {
                in.readerIndex(readerIndex);
                return;
            }
            
            ByteBuf payload = in.readSlice(size);
            byte frameEnd = in.readByte();
            
            if (frameEnd != AmqpFrame.FRAME_END) {
                throw new IllegalArgumentException("Invalid frame end marker");
            }
            
            AmqpFrame frame = new AmqpFrame(type, channel, payload.retain());
            out.add(frame);
        }
    }
    
    public static class AmqpFrameEncoder extends MessageToByteEncoder<AmqpFrame> {
        
        @Override
        protected void encode(ChannelHandlerContext ctx, AmqpFrame frame, ByteBuf out) throws Exception {
            out.writeByte(frame.getType());
            out.writeShort(frame.getChannel());
            out.writeInt(frame.getSize());
            out.writeBytes(frame.getPayload());
            out.writeByte(AmqpFrame.FRAME_END);
        }
    }
    
    public static class ProtocolHeaderDecoder extends ByteToMessageDecoder {
        private static final byte[] AMQP_PROTOCOL_HEADER = {
            'A', 'M', 'Q', 'P', 0, 0, 9, 1
        };
        
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            if (in.readableBytes() < 8) {
                return;
            }
            
            byte[] header = new byte[8];
            in.readBytes(header);
            
            for (int i = 0; i < 8; i++) {
                if (header[i] != AMQP_PROTOCOL_HEADER[i]) {
                    throw new IllegalArgumentException("Invalid AMQP protocol header");
                }
            }
            
            ctx.pipeline().remove(this);
            ctx.pipeline().addLast(new AmqpFrameDecoder());
        }
    }
    
    public static ByteBuf encodeShortString(ByteBuf buf, String value) {
        if (value == null) value = "";
        byte[] bytes = value.getBytes();
        buf.writeByte(bytes.length);
        buf.writeBytes(bytes);
        return buf;
    }
    
    public static String decodeShortString(ByteBuf buf) {
        int length = buf.readUnsignedByte();
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return new String(bytes);
    }
    
    public static ByteBuf encodeLongString(ByteBuf buf, String value) {
        if (value == null) value = "";
        byte[] bytes = value.getBytes();
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
        return buf;
    }
    
    public static String decodeLongString(ByteBuf buf) {
        int length = buf.readInt();
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return new String(bytes);
    }
    
    public static ByteBuf encodeBoolean(ByteBuf buf, boolean value) {
        buf.writeByte(value ? 1 : 0);
        return buf;
    }
    
    public static boolean decodeBoolean(ByteBuf buf) {
        return buf.readByte() != 0;
    }
}