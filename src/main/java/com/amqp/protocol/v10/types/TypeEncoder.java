package com.amqp.protocol.v10.types;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Encoder for AMQP 1.0 types.
 * Encodes Java objects to AMQP 1.0 wire format.
 */
public class TypeEncoder {

    /**
     * Encode any object to AMQP 1.0 format.
     */
    public static void encode(Object value, ByteBuf buffer) {
        if (value == null) {
            encodeNull(buffer);
        } else if (value instanceof Boolean) {
            encodeBoolean((Boolean) value, buffer);
        } else if (value instanceof UByte) {
            encodeUbyte(((UByte) value).intValue(), buffer);
        } else if (value instanceof UShort) {
            encodeUshort(((UShort) value).intValue(), buffer);
        } else if (value instanceof UInt) {
            encodeUint(((UInt) value).longValue(), buffer);
        } else if (value instanceof Byte) {
            encodeByte((Byte) value, buffer);
        } else if (value instanceof Short) {
            encodeShort((Short) value, buffer);
        } else if (value instanceof Integer) {
            encodeInt((Integer) value, buffer);
        } else if (value instanceof Long) {
            encodeLong((Long) value, buffer);
        } else if (value instanceof Float) {
            encodeFloat((Float) value, buffer);
        } else if (value instanceof Double) {
            encodeDouble((Double) value, buffer);
        } else if (value instanceof String) {
            encodeString((String) value, buffer);
        } else if (value instanceof byte[]) {
            encodeBinary((byte[]) value, buffer);
        } else if (value instanceof UUID) {
            encodeUuid((UUID) value, buffer);
        } else if (value instanceof Symbol) {
            encodeSymbol((Symbol) value, buffer);
        } else if (value instanceof Date) {
            encodeTimestamp((Date) value, buffer);
        } else if (value instanceof List) {
            encodeList((List<?>) value, buffer);
        } else if (value instanceof Map) {
            encodeMap((Map<?, ?>) value, buffer);
        } else if (value instanceof DescribedType) {
            encodeDescribed((DescribedType) value, buffer);
        } else if (value.getClass().isArray()) {
            encodeArray(value, buffer);
        } else {
            throw new IllegalArgumentException("Cannot encode type: " + value.getClass());
        }
    }

    public static void encodeNull(ByteBuf buffer) {
        buffer.writeByte(AmqpType.FormatCode.NULL);
    }

    public static void encodeBoolean(boolean value, ByteBuf buffer) {
        buffer.writeByte(value ? AmqpType.FormatCode.BOOLEAN_TRUE : AmqpType.FormatCode.BOOLEAN_FALSE);
    }

    public static void encodeByte(byte value, ByteBuf buffer) {
        buffer.writeByte(AmqpType.FormatCode.BYTE);
        buffer.writeByte(value);
    }

    public static void encodeUbyte(int value, ByteBuf buffer) {
        buffer.writeByte(AmqpType.FormatCode.UBYTE);
        buffer.writeByte(value & 0xFF);
    }

    public static void encodeShort(short value, ByteBuf buffer) {
        buffer.writeByte(AmqpType.FormatCode.SHORT);
        buffer.writeShort(value);
    }

    public static void encodeUshort(int value, ByteBuf buffer) {
        buffer.writeByte(AmqpType.FormatCode.USHORT);
        buffer.writeShort(value & 0xFFFF);
    }

    public static void encodeInt(int value, ByteBuf buffer) {
        if (value >= -128 && value <= 127) {
            buffer.writeByte(AmqpType.FormatCode.SMALL_INT);
            buffer.writeByte(value);
        } else {
            buffer.writeByte(AmqpType.FormatCode.INT);
            buffer.writeInt(value);
        }
    }

    public static void encodeUint(long value, ByteBuf buffer) {
        if (value == 0) {
            buffer.writeByte(AmqpType.FormatCode.UINT_0);
        } else if (value <= 255) {
            buffer.writeByte(AmqpType.FormatCode.SMALL_UINT);
            buffer.writeByte((int) value);
        } else {
            buffer.writeByte(AmqpType.FormatCode.UINT);
            buffer.writeInt((int) value);
        }
    }

    public static void encodeLong(long value, ByteBuf buffer) {
        if (value >= -128 && value <= 127) {
            buffer.writeByte(AmqpType.FormatCode.SMALL_LONG);
            buffer.writeByte((int) value);
        } else {
            buffer.writeByte(AmqpType.FormatCode.LONG);
            buffer.writeLong(value);
        }
    }

    public static void encodeUlong(long value, ByteBuf buffer) {
        if (value == 0) {
            buffer.writeByte(AmqpType.FormatCode.ULONG_0);
        } else if (value > 0 && value <= 255) {
            buffer.writeByte(AmqpType.FormatCode.SMALL_ULONG);
            buffer.writeByte((int) value);
        } else {
            buffer.writeByte(AmqpType.FormatCode.ULONG);
            buffer.writeLong(value);
        }
    }

    public static void encodeFloat(float value, ByteBuf buffer) {
        buffer.writeByte(AmqpType.FormatCode.FLOAT);
        buffer.writeFloat(value);
    }

    public static void encodeDouble(double value, ByteBuf buffer) {
        buffer.writeByte(AmqpType.FormatCode.DOUBLE);
        buffer.writeDouble(value);
    }

    public static void encodeTimestamp(Date value, ByteBuf buffer) {
        buffer.writeByte(AmqpType.FormatCode.TIMESTAMP);
        buffer.writeLong(value.getTime());
    }

    public static void encodeUuid(UUID value, ByteBuf buffer) {
        buffer.writeByte(AmqpType.FormatCode.UUID);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());
    }

    public static void encodeBinary(byte[] value, ByteBuf buffer) {
        if (value.length <= 255) {
            buffer.writeByte(AmqpType.FormatCode.VBIN8);
            buffer.writeByte(value.length);
        } else {
            buffer.writeByte(AmqpType.FormatCode.VBIN32);
            buffer.writeInt(value.length);
        }
        buffer.writeBytes(value);
    }

    public static void encodeString(String value, ByteBuf buffer) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= 255) {
            buffer.writeByte(AmqpType.FormatCode.STR8_UTF8);
            buffer.writeByte(bytes.length);
        } else {
            buffer.writeByte(AmqpType.FormatCode.STR32_UTF8);
            buffer.writeInt(bytes.length);
        }
        buffer.writeBytes(bytes);
    }

    public static void encodeSymbol(Symbol value, ByteBuf buffer) {
        byte[] bytes = value.toString().getBytes(StandardCharsets.US_ASCII);
        if (bytes.length <= 255) {
            buffer.writeByte(AmqpType.FormatCode.SYM8);
            buffer.writeByte(bytes.length);
        } else {
            buffer.writeByte(AmqpType.FormatCode.SYM32);
            buffer.writeInt(bytes.length);
        }
        buffer.writeBytes(bytes);
    }

    public static void encodeList(List<?> value, ByteBuf buffer) {
        if (value.isEmpty()) {
            buffer.writeByte(AmqpType.FormatCode.LIST_0);
            return;
        }

        // Encode elements to temporary buffer to get size
        ByteBuf tempBuffer = buffer.alloc().buffer();
        try {
            for (Object element : value) {
                encode(element, tempBuffer);
            }

            int count = value.size();
            int size = tempBuffer.readableBytes();

            if (count <= 255 && size <= 255) {
                buffer.writeByte(AmqpType.FormatCode.LIST8);
                buffer.writeByte(size + 1); // size includes count
                buffer.writeByte(count);
            } else {
                buffer.writeByte(AmqpType.FormatCode.LIST32);
                buffer.writeInt(size + 4); // size includes count
                buffer.writeInt(count);
            }
            buffer.writeBytes(tempBuffer);
        } finally {
            tempBuffer.release();
        }
    }

    public static void encodeMap(Map<?, ?> value, ByteBuf buffer) {
        if (value.isEmpty()) {
            buffer.writeByte(AmqpType.FormatCode.MAP8);
            buffer.writeByte(1); // size
            buffer.writeByte(0); // count
            return;
        }

        // Encode elements to temporary buffer
        ByteBuf tempBuffer = buffer.alloc().buffer();
        try {
            for (Map.Entry<?, ?> entry : value.entrySet()) {
                encode(entry.getKey(), tempBuffer);
                encode(entry.getValue(), tempBuffer);
            }

            int count = value.size() * 2; // key-value pairs
            int size = tempBuffer.readableBytes();

            if (count <= 255 && size <= 255) {
                buffer.writeByte(AmqpType.FormatCode.MAP8);
                buffer.writeByte(size + 1);
                buffer.writeByte(count);
            } else {
                buffer.writeByte(AmqpType.FormatCode.MAP32);
                buffer.writeInt(size + 4);
                buffer.writeInt(count);
            }
            buffer.writeBytes(tempBuffer);
        } finally {
            tempBuffer.release();
        }
    }

    public static void encodeArray(Object array, ByteBuf buffer) {
        // byte[] is handled as binary, not array
        if (array instanceof byte[]) {
            encodeBinary((byte[]) array, buffer);
            return;
        }

        // Handle Symbol[] specifically for SASL mechanisms
        if (array instanceof Symbol[]) {
            encodeSymbolArray((Symbol[]) array, buffer);
            return;
        }

        // Handle other Object arrays as lists for compatibility
        List<Object> list = new ArrayList<>();
        if (array instanceof Object[]) {
            list.addAll(Arrays.asList((Object[]) array));
        } else if (array instanceof int[]) {
            for (int v : (int[]) array) list.add(v);
        } else if (array instanceof long[]) {
            for (long v : (long[]) array) list.add(v);
        }
        // For other types, encode as lists
        encodeList(list, buffer);
    }

    /**
     * Encode a Symbol array using AMQP 1.0 array format.
     * This is required for SASL mechanisms which expects symbol[].
     */
    public static void encodeSymbolArray(Symbol[] symbols, ByteBuf buffer) {
        if (symbols.length == 0) {
            // Empty array - use list0 format for compatibility
            buffer.writeByte(AmqpType.FormatCode.LIST_0);
            return;
        }

        // Encode symbols to temporary buffer to get size
        ByteBuf tempBuffer = buffer.alloc().buffer();
        try {
            // Determine if we need sym8 or sym32 format
            boolean useSmallFormat = true;
            for (Symbol s : symbols) {
                byte[] bytes = s.toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII);
                if (bytes.length > 255) {
                    useSmallFormat = false;
                    break;
                }
            }

            // Write element constructor and data
            byte elementConstructor = useSmallFormat ?
                    AmqpType.FormatCode.SYM8 : AmqpType.FormatCode.SYM32;

            for (Symbol s : symbols) {
                byte[] bytes = s.toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII);
                if (useSmallFormat) {
                    tempBuffer.writeByte(bytes.length);
                } else {
                    tempBuffer.writeInt(bytes.length);
                }
                tempBuffer.writeBytes(bytes);
            }

            int count = symbols.length;
            // Size includes: 1 byte for element constructor + element data
            int size = 1 + tempBuffer.readableBytes();

            if (count <= 255 && size <= 255) {
                buffer.writeByte(AmqpType.FormatCode.ARRAY8);
                buffer.writeByte(size);
                buffer.writeByte(count);
            } else {
                buffer.writeByte(AmqpType.FormatCode.ARRAY32);
                buffer.writeInt(size);
                buffer.writeInt(count);
            }
            buffer.writeByte(elementConstructor);
            buffer.writeBytes(tempBuffer);
        } finally {
            tempBuffer.release();
        }
    }

    public static void encodeDescribed(DescribedType value, ByteBuf buffer) {
        buffer.writeByte(AmqpType.FormatCode.DESCRIBED);
        // Descriptors are always ulong in AMQP 1.0
        Object descriptor = value.getDescriptor();
        if (descriptor instanceof Number) {
            encodeUlong(((Number) descriptor).longValue(), buffer);
        } else {
            encode(descriptor, buffer);
        }
        encode(value.getDescribed(), buffer);
    }

    /**
     * Encode a descriptor code (ulong).
     */
    public static void encodeDescriptor(long descriptor, ByteBuf buffer) {
        buffer.writeByte(AmqpType.FormatCode.DESCRIBED);
        encodeUlong(descriptor, buffer);
    }

    /**
     * Encode a performative or section with its descriptor and fields.
     */
    public static void encodeDescribedList(long descriptor, List<Object> fields, ByteBuf buffer) {
        buffer.writeByte(AmqpType.FormatCode.DESCRIBED);
        encodeUlong(descriptor, buffer);
        encodeList(fields, buffer);
    }

    /**
     * Calculate encoded size without actually encoding.
     */
    public static int getEncodedSize(Object value) {
        if (value == null) return 1;
        if (value instanceof Boolean) return 1;
        if (value instanceof Byte) return 2;
        if (value instanceof Short) return 3;
        if (value instanceof Integer) {
            int v = (Integer) value;
            return (v >= -128 && v <= 127) ? 2 : 5;
        }
        if (value instanceof Long) {
            long v = (Long) value;
            return (v >= -128 && v <= 127) ? 2 : 9;
        }
        if (value instanceof Float) return 5;
        if (value instanceof Double) return 9;
        if (value instanceof String) {
            byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
            return bytes.length <= 255 ? 2 + bytes.length : 5 + bytes.length;
        }
        if (value instanceof byte[]) {
            int len = ((byte[]) value).length;
            return len <= 255 ? 2 + len : 5 + len;
        }
        if (value instanceof UUID) return 17;
        // For complex types, estimate
        return 10;
    }
}
