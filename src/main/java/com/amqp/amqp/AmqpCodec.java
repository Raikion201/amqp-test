package com.amqp.amqp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AmqpCodec {

    // Security constants - prevent denial of service attacks
    public static final int MAX_FRAME_SIZE = 1024 * 1024; // 1MB max frame
    public static final int MAX_SHORT_STRING_LENGTH = 255;
    public static final int MAX_LONG_STRING_LENGTH = 256 * 1024; // 256KB max string

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

            // Security: Validate frame size to prevent DoS attacks
            if (size < 0 || size > MAX_FRAME_SIZE) {
                throw new IllegalArgumentException(
                    "Invalid frame size: " + size + " (max: " + MAX_FRAME_SIZE + ")");
            }

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
        private static final Logger logger = LoggerFactory.getLogger(AmqpFrameEncoder.class);

        @Override
        protected void encode(ChannelHandlerContext ctx, AmqpFrame frame, ByteBuf out) throws Exception {
            logger.debug("Encoding frame: type={}, channel={}, size={}", frame.getType(), frame.getChannel(), frame.getSize());
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

            // Replace this decoder with the frame decoder
            ctx.pipeline().addAfter(ctx.name(), "frameDecoder", new AmqpFrameDecoder());
            ctx.pipeline().remove(this);

            // Fire event to notify that protocol header was received successfully
            // This signals the connection handler to send Connection.Start
            ctx.fireUserEventTriggered(ProtocolHeaderReceivedEvent.INSTANCE);
        }
    }
    
    public static ByteBuf encodeShortString(ByteBuf buf, String value) {
        if (value == null) value = "";
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        // Security: Validate string length fits in a byte
        if (bytes.length > MAX_SHORT_STRING_LENGTH) {
            throw new IllegalArgumentException(
                "Short string too long: " + bytes.length + " bytes (max: " + MAX_SHORT_STRING_LENGTH + ")");
        }
        buf.writeByte(bytes.length);
        buf.writeBytes(bytes);
        return buf;
    }

    public static String decodeShortString(ByteBuf buf) {
        int length = buf.readUnsignedByte();
        // Security: Validate buffer has enough bytes
        if (buf.readableBytes() < length) {
            throw new IllegalArgumentException(
                "Buffer underflow: need " + length + " bytes but only " + buf.readableBytes() + " available");
        }
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    public static ByteBuf encodeLongString(ByteBuf buf, String value) {
        if (value == null) value = "";
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
        return buf;
    }

    public static String decodeLongString(ByteBuf buf) {
        int length = buf.readInt();
        // Security: Validate length to prevent DoS via unbounded allocation
        if (length < 0 || length > MAX_LONG_STRING_LENGTH) {
            throw new IllegalArgumentException(
                "Invalid long string length: " + length + " (max: " + MAX_LONG_STRING_LENGTH + ")");
        }
        // Security: Validate buffer has enough bytes
        if (buf.readableBytes() < length) {
            throw new IllegalArgumentException(
                "Buffer underflow: need " + length + " bytes but only " + buf.readableBytes() + " available");
        }
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
    
    public static ByteBuf encodeBoolean(ByteBuf buf, boolean value) {
        buf.writeByte(value ? 1 : 0);
        return buf;
    }
    
    public static boolean decodeBoolean(ByteBuf buf) {
        return buf.readByte() != 0;
    }

    /**
     * Decode an AMQP field table from the buffer.
     * Format: 4-byte length + table entries
     */
    public static java.util.Map<String, Object> decodeTable(ByteBuf buf) {
        java.util.Map<String, Object> table = new java.util.HashMap<>();

        int tableLength = buf.readInt();
        if (tableLength <= 0) {
            return table;
        }

        // Security: validate table length
        if (tableLength > buf.readableBytes()) {
            throw new IllegalArgumentException(
                "Table length exceeds available bytes: " + tableLength + " > " + buf.readableBytes());
        }

        int endIndex = buf.readerIndex() + tableLength;

        while (buf.readerIndex() < endIndex) {
            String key = decodeShortString(buf);
            Object value = decodeFieldValue(buf);
            table.put(key, value);
        }

        return table;
    }

    /**
     * Decode a single field value from the buffer.
     * The first byte is the type indicator.
     */
    public static Object decodeFieldValue(ByteBuf buf) {
        byte type = buf.readByte();

        switch (type) {
            case 't': // Boolean
                return buf.readByte() != 0;
            case 'b': // Signed byte
                return buf.readByte();
            case 'B': // Unsigned byte
                return buf.readUnsignedByte();
            case 's': // Signed short
                return buf.readShort();
            case 'u': // Unsigned short
                return buf.readUnsignedShort();
            case 'I': // Signed 32-bit
                return buf.readInt();
            case 'i': // Unsigned 32-bit (stored as long)
                return buf.readUnsignedInt();
            case 'l': // Signed 64-bit
                return buf.readLong();
            case 'f': // 32-bit float
                return buf.readFloat();
            case 'd': // 64-bit double
                return buf.readDouble();
            case 'D': // Decimal value (scale + unscaled value)
                byte scale = buf.readByte();
                int unscaled = buf.readInt();
                return new java.math.BigDecimal(unscaled).scaleByPowerOfTen(-scale);
            case 'S': // Long string
                return decodeLongString(buf);
            case 'A': // Field array
                return decodeFieldArray(buf);
            case 'T': // Timestamp (64-bit)
                return buf.readLong();
            case 'F': // Nested table
                return decodeTable(buf);
            case 'V': // Void/null
                return null;
            case 'x': // Byte array
                int len = buf.readInt();
                byte[] bytes = new byte[len];
                buf.readBytes(bytes);
                return bytes;
            default:
                throw new IllegalArgumentException("Unknown field type: " + (char) type);
        }
    }

    /**
     * Decode a field array from the buffer.
     */
    public static java.util.List<Object> decodeFieldArray(ByteBuf buf) {
        java.util.List<Object> array = new java.util.ArrayList<>();
        int arrayLength = buf.readInt();

        if (arrayLength <= 0) {
            return array;
        }

        int endIndex = buf.readerIndex() + arrayLength;

        while (buf.readerIndex() < endIndex) {
            array.add(decodeFieldValue(buf));
        }

        return array;
    }

    /**
     * Encode an AMQP field table to the buffer.
     */
    public static void encodeTable(ByteBuf buf, java.util.Map<String, Object> table) {
        if (table == null || table.isEmpty()) {
            buf.writeInt(0);
            return;
        }

        // Write to a temporary buffer to calculate size
        io.netty.buffer.ByteBuf tempBuf = io.netty.buffer.Unpooled.buffer();
        try {
            for (java.util.Map.Entry<String, Object> entry : table.entrySet()) {
                encodeShortString(tempBuf, entry.getKey());
                encodeFieldValue(tempBuf, entry.getValue());
            }

            // Write length and content
            buf.writeInt(tempBuf.readableBytes());
            buf.writeBytes(tempBuf);
        } finally {
            tempBuf.release();
        }
    }

    /**
     * Encode a single field value to the buffer.
     */
    public static void encodeFieldValue(ByteBuf buf, Object value) {
        if (value == null) {
            buf.writeByte('V');
        } else if (value instanceof Boolean) {
            buf.writeByte('t');
            buf.writeByte((Boolean) value ? 1 : 0);
        } else if (value instanceof Byte) {
            buf.writeByte('b');
            buf.writeByte((Byte) value);
        } else if (value instanceof Short) {
            buf.writeByte('s');
            buf.writeShort((Short) value);
        } else if (value instanceof Integer) {
            buf.writeByte('I');
            buf.writeInt((Integer) value);
        } else if (value instanceof Long) {
            buf.writeByte('l');
            buf.writeLong((Long) value);
        } else if (value instanceof Float) {
            buf.writeByte('f');
            buf.writeFloat((Float) value);
        } else if (value instanceof Double) {
            buf.writeByte('d');
            buf.writeDouble((Double) value);
        } else if (value instanceof String) {
            buf.writeByte('S');
            encodeLongString(buf, (String) value);
        } else if (value instanceof byte[]) {
            buf.writeByte('x');
            byte[] bytes = (byte[]) value;
            buf.writeInt(bytes.length);
            buf.writeBytes(bytes);
        } else if (value instanceof java.util.Map) {
            buf.writeByte('F');
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> nestedTable = (java.util.Map<String, Object>) value;
            encodeTable(buf, nestedTable);
        } else if (value instanceof java.util.List) {
            buf.writeByte('A');
            @SuppressWarnings("unchecked")
            java.util.List<Object> list = (java.util.List<Object>) value;
            encodeFieldArray(buf, list);
        } else if (value instanceof java.util.Date) {
            buf.writeByte('T');
            buf.writeLong(((java.util.Date) value).getTime() / 1000); // AMQP timestamp is in seconds
        } else if (value instanceof java.math.BigDecimal) {
            buf.writeByte('D');
            java.math.BigDecimal decimal = (java.math.BigDecimal) value;
            buf.writeByte(decimal.scale());
            buf.writeInt(decimal.unscaledValue().intValue());
        } else {
            // Default to string representation
            buf.writeByte('S');
            encodeLongString(buf, value.toString());
        }
    }

    /**
     * Encode a field array to the buffer.
     */
    public static void encodeFieldArray(ByteBuf buf, java.util.List<Object> array) {
        if (array == null || array.isEmpty()) {
            buf.writeInt(0);
            return;
        }

        io.netty.buffer.ByteBuf tempBuf = io.netty.buffer.Unpooled.buffer();
        try {
            for (Object item : array) {
                encodeFieldValue(tempBuf, item);
            }

            buf.writeInt(tempBuf.readableBytes());
            buf.writeBytes(tempBuf);
        } finally {
            tempBuf.release();
        }
    }
}