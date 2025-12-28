package com.amqp.protocol.v10.types;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Decoder for AMQP 1.0 types.
 * Decodes AMQP 1.0 wire format to Java objects.
 */
public class TypeDecoder {

    // Security constants - prevent denial of service attacks
    public static final int MAX_BINARY_SIZE = 16 * 1024 * 1024; // 16MB max binary
    public static final int MAX_STRING_SIZE = 1024 * 1024; // 1MB max string
    public static final int MAX_COLLECTION_SIZE = 10000; // Max elements in list/map/array

    /**
     * Decode any AMQP 1.0 value from the buffer.
     */
    public static Object decode(ByteBuf buffer) {
        if (!buffer.isReadable()) {
            return null;
        }

        byte formatCode = buffer.readByte();
        return decodeFormatCode(formatCode, buffer);
    }

    private static Object decodeFormatCode(byte formatCode, ByteBuf buffer) {
        switch (formatCode) {
            // Described type
            case AmqpType.FormatCode.DESCRIBED:
                return decodeDescribed(buffer);

            // Null
            case AmqpType.FormatCode.NULL:
                return null;

            // Boolean
            case AmqpType.FormatCode.BOOLEAN_TRUE:
                return Boolean.TRUE;
            case AmqpType.FormatCode.BOOLEAN_FALSE:
                return Boolean.FALSE;
            case AmqpType.FormatCode.BOOLEAN:
                return buffer.readByte() != 0;

            // Unsigned integers
            case AmqpType.FormatCode.UBYTE:
                return (short) (buffer.readByte() & 0xFF);
            case AmqpType.FormatCode.USHORT:
                return buffer.readUnsignedShort();
            case AmqpType.FormatCode.UINT:
                return buffer.readUnsignedInt();
            case AmqpType.FormatCode.UINT_0:
                return 0L;
            case AmqpType.FormatCode.SMALL_UINT:
                return (long) (buffer.readByte() & 0xFF);
            case AmqpType.FormatCode.ULONG:
                return buffer.readLong(); // Treat as signed for Java
            case AmqpType.FormatCode.ULONG_0:
                return 0L;
            case AmqpType.FormatCode.SMALL_ULONG:
                return (long) (buffer.readByte() & 0xFF);

            // Signed integers
            case AmqpType.FormatCode.BYTE:
                return buffer.readByte();
            case AmqpType.FormatCode.SHORT:
                return buffer.readShort();
            case AmqpType.FormatCode.INT:
                return buffer.readInt();
            case AmqpType.FormatCode.SMALL_INT:
                return (int) buffer.readByte();
            case AmqpType.FormatCode.LONG:
                return buffer.readLong();
            case AmqpType.FormatCode.SMALL_LONG:
                return (long) buffer.readByte();

            // Floating point
            case AmqpType.FormatCode.FLOAT:
                return buffer.readFloat();
            case AmqpType.FormatCode.DOUBLE:
                return buffer.readDouble();

            // Timestamp
            case AmqpType.FormatCode.TIMESTAMP:
                return new Date(buffer.readLong());

            // UUID
            case AmqpType.FormatCode.UUID:
                return new UUID(buffer.readLong(), buffer.readLong());

            // Binary
            case AmqpType.FormatCode.VBIN8:
                return readBytes(buffer, buffer.readUnsignedByte());
            case AmqpType.FormatCode.VBIN32:
                return readBytes(buffer, buffer.readInt());

            // String
            case AmqpType.FormatCode.STR8_UTF8:
                return readString(buffer, buffer.readUnsignedByte());
            case AmqpType.FormatCode.STR32_UTF8:
                return readString(buffer, buffer.readInt());

            // Symbol
            case AmqpType.FormatCode.SYM8:
                return Symbol.valueOf(readAsciiString(buffer, buffer.readUnsignedByte()));
            case AmqpType.FormatCode.SYM32:
                return Symbol.valueOf(readAsciiString(buffer, buffer.readInt()));

            // List
            case AmqpType.FormatCode.LIST_0:
                return new ArrayList<>();
            case AmqpType.FormatCode.LIST8:
                return decodeList8(buffer);
            case AmqpType.FormatCode.LIST32:
                return decodeList32(buffer);

            // Map
            case AmqpType.FormatCode.MAP8:
                return decodeMap8(buffer);
            case AmqpType.FormatCode.MAP32:
                return decodeMap32(buffer);

            // Array
            case AmqpType.FormatCode.ARRAY8:
                return decodeArray8(buffer);
            case (byte) 0xF0: // ARRAY32
                return decodeArray32(buffer);

            // Decimal types (simplified - return as bytes)
            case AmqpType.FormatCode.DECIMAL32:
                return readBytes(buffer, 4);
            case AmqpType.FormatCode.DECIMAL64:
                return readBytes(buffer, 8);
            case AmqpType.FormatCode.DECIMAL128:
                return readBytes(buffer, 16);

            // Char
            case AmqpType.FormatCode.CHAR:
                return (char) buffer.readInt();

            default:
                throw new IllegalArgumentException("Unknown format code: 0x" +
                    Integer.toHexString(formatCode & 0xFF));
        }
    }

    private static byte[] readBytes(ByteBuf buffer, int length) {
        // Security: Validate length to prevent DoS via unbounded allocation
        if (length < 0 || length > MAX_BINARY_SIZE) {
            throw new IllegalArgumentException(
                "Invalid binary length: " + length + " (max: " + MAX_BINARY_SIZE + ")");
        }
        // Security: Validate buffer has enough bytes
        if (buffer.readableBytes() < length) {
            throw new IllegalArgumentException(
                "Buffer underflow: need " + length + " bytes but only " + buffer.readableBytes() + " available");
        }
        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);
        return bytes;
    }

    private static String readString(ByteBuf buffer, int length) {
        // Security: Validate length to prevent DoS via unbounded allocation
        if (length < 0 || length > MAX_STRING_SIZE) {
            throw new IllegalArgumentException(
                "Invalid string length: " + length + " (max: " + MAX_STRING_SIZE + ")");
        }
        // Security: Validate buffer has enough bytes
        if (buffer.readableBytes() < length) {
            throw new IllegalArgumentException(
                "Buffer underflow: need " + length + " bytes but only " + buffer.readableBytes() + " available");
        }
        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String readAsciiString(ByteBuf buffer, int length) {
        // Security: Validate length to prevent DoS via unbounded allocation
        if (length < 0 || length > MAX_STRING_SIZE) {
            throw new IllegalArgumentException(
                "Invalid symbol length: " + length + " (max: " + MAX_STRING_SIZE + ")");
        }
        // Security: Validate buffer has enough bytes
        if (buffer.readableBytes() < length) {
            throw new IllegalArgumentException(
                "Buffer underflow: need " + length + " bytes but only " + buffer.readableBytes() + " available");
        }
        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    private static DescribedType decodeDescribed(ByteBuf buffer) {
        Object descriptor = decode(buffer);
        Object described = decode(buffer);
        return new DescribedType.Default(descriptor, described);
    }

    private static List<Object> decodeList8(ByteBuf buffer) {
        int size = buffer.readUnsignedByte();
        int count = buffer.readUnsignedByte();
        // Security: Validate collection size (even for 8-bit, enforce limits)
        if (count > MAX_COLLECTION_SIZE) {
            throw new IllegalArgumentException(
                "Invalid list count: " + count + " (max: " + MAX_COLLECTION_SIZE + ")");
        }
        return decodeListElements(buffer, count);
    }

    private static List<Object> decodeList32(ByteBuf buffer) {
        int size = buffer.readInt();
        int count = buffer.readInt();
        // Security: Validate collection size
        if (count < 0 || count > MAX_COLLECTION_SIZE) {
            throw new IllegalArgumentException(
                "Invalid list count: " + count + " (max: " + MAX_COLLECTION_SIZE + ")");
        }
        return decodeListElements(buffer, count);
    }

    private static List<Object> decodeListElements(ByteBuf buffer, int count) {
        List<Object> list = new ArrayList<>(Math.min(count, 256)); // Don't pre-allocate huge arrays
        for (int i = 0; i < count; i++) {
            list.add(decode(buffer));
        }
        return list;
    }

    private static Map<Object, Object> decodeMap8(ByteBuf buffer) {
        int size = buffer.readUnsignedByte();
        int count = buffer.readUnsignedByte();
        // Security: Validate collection size (even for 8-bit, enforce limits)
        if (count > MAX_COLLECTION_SIZE * 2) {
            throw new IllegalArgumentException(
                "Invalid map count: " + count + " (max: " + (MAX_COLLECTION_SIZE * 2) + ")");
        }
        return decodeMapElements(buffer, count / 2);
    }

    private static Map<Object, Object> decodeMap32(ByteBuf buffer) {
        int size = buffer.readInt();
        int count = buffer.readInt();
        // Security: Validate collection size
        if (count < 0 || count > MAX_COLLECTION_SIZE * 2) {
            throw new IllegalArgumentException(
                "Invalid map count: " + count + " (max: " + (MAX_COLLECTION_SIZE * 2) + ")");
        }
        return decodeMapElements(buffer, count / 2);
    }

    private static Map<Object, Object> decodeMapElements(ByteBuf buffer, int pairCount) {
        Map<Object, Object> map = new LinkedHashMap<>(Math.min(pairCount, 256)); // Don't pre-allocate huge maps
        for (int i = 0; i < pairCount; i++) {
            Object key = decode(buffer);
            Object value = decode(buffer);
            map.put(key, value);
        }
        return map;
    }

    private static Object[] decodeArray8(ByteBuf buffer) {
        int size = buffer.readUnsignedByte();
        int count = buffer.readUnsignedByte();
        // Security: Validate collection size (even for 8-bit, enforce limits)
        if (count > MAX_COLLECTION_SIZE) {
            throw new IllegalArgumentException(
                "Invalid array count: " + count + " (max: " + MAX_COLLECTION_SIZE + ")");
        }
        byte elementType = buffer.readByte();
        return decodeArrayElements(buffer, count, elementType);
    }

    private static Object[] decodeArray32(ByteBuf buffer) {
        int size = buffer.readInt();
        int count = buffer.readInt();
        // Security: Validate collection size
        if (count < 0 || count > MAX_COLLECTION_SIZE) {
            throw new IllegalArgumentException(
                "Invalid array count: " + count + " (max: " + MAX_COLLECTION_SIZE + ")");
        }
        byte elementType = buffer.readByte();
        return decodeArrayElements(buffer, count, elementType);
    }

    private static Object[] decodeArrayElements(ByteBuf buffer, int count, byte elementType) {
        // Security: Don't pre-allocate huge arrays
        Object[] array = new Object[Math.min(count, MAX_COLLECTION_SIZE)];
        for (int i = 0; i < count; i++) {
            array[i] = decodeFormatCode(elementType, buffer);
        }
        return array;
    }

    /**
     * Decode a list of fields for a performative or section.
     */
    @SuppressWarnings("unchecked")
    public static List<Object> decodeFields(DescribedType described) {
        Object value = described.getDescribed();
        if (value instanceof List) {
            return (List<Object>) value;
        }
        return Collections.emptyList();
    }

    /**
     * Get a field from a decoded list, with null handling.
     */
    public static Object getField(List<Object> fields, int index) {
        if (fields == null || index >= fields.size()) {
            return null;
        }
        return fields.get(index);
    }

    /**
     * Get a field as a specific type.
     */
    @SuppressWarnings("unchecked")
    public static <T> T getField(List<Object> fields, int index, Class<T> type) {
        Object value = getField(fields, index);
        if (value == null) {
            return null;
        }
        if (type.isInstance(value)) {
            return (T) value;
        }
        // Handle numeric conversions
        if (type == Long.class && value instanceof Number) {
            return (T) Long.valueOf(((Number) value).longValue());
        }
        if (type == Integer.class && value instanceof Number) {
            return (T) Integer.valueOf(((Number) value).intValue());
        }
        if (type == String.class && value instanceof Symbol) {
            return (T) value.toString();
        }
        throw new ClassCastException("Cannot cast " + value.getClass() + " to " + type);
    }

    /**
     * Get a field as a boolean with default value.
     */
    public static boolean getBooleanField(List<Object> fields, int index, boolean defaultValue) {
        Object value = getField(fields, index);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return defaultValue;
    }

    /**
     * Get a field as a long with default value.
     */
    public static long getLongField(List<Object> fields, int index, long defaultValue) {
        Object value = getField(fields, index);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return defaultValue;
    }

    /**
     * Get a field as an int with default value.
     */
    public static int getIntField(List<Object> fields, int index, int defaultValue) {
        Object value = getField(fields, index);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return defaultValue;
    }
}
