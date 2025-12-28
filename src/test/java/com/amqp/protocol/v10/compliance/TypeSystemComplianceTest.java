/*
 * AMQP 1.0 Type System Compliance Tests
 *
 * Tests compliance with OASIS AMQP Version 1.0, Part 1: Types
 * https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html
 *
 * Adapted from Apache Qpid ProtonJ2 codec tests.
 * Licensed under the Apache License, Version 2.0
 * Original: https://github.com/apache/qpid-protonj2
 */
package com.amqp.protocol.v10.compliance;

import com.amqp.protocol.v10.types.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP 1.0 Type System Compliance Tests.
 *
 * Verifies compliance with OASIS AMQP Version 1.0, Part 1: Types.
 * Section references correspond to the specification.
 *
 * @see <a href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html">AMQP 1.0 Types</a>
 */
@DisplayName("AMQP 1.0 Type System Compliance (OASIS Part 1)")
public class TypeSystemComplianceTest {

    // Section 1.2: Primitive Type Definitions

    @Nested
    @DisplayName("Section 1.2 - Primitive Types")
    class PrimitiveTypesCompliance {

        @Test
        @DisplayName("1.2.1: null - absence of a value (0x40)")
        void testNullEncoding() {
            // null is encoded as constructor 0x40 with no data
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeByte(0x40);

            Object decoded = TypeDecoder.decode(buffer);
            assertNull(decoded, "0x40 should decode to null");
        }

        @Test
        @DisplayName("1.2.2: boolean - true/false (0x41/0x42)")
        void testBooleanEncoding() {
            // true is encoded as 0x41
            ByteBuf trueBuffer = Unpooled.buffer();
            trueBuffer.writeByte(0x41);
            assertEquals(Boolean.TRUE, TypeDecoder.decode(trueBuffer));

            // false is encoded as 0x42
            ByteBuf falseBuffer = Unpooled.buffer();
            falseBuffer.writeByte(0x42);
            assertEquals(Boolean.FALSE, TypeDecoder.decode(falseBuffer));
        }

        @Test
        @DisplayName("1.2.3: ubyte - 8-bit unsigned (0x50)")
        void testUbyteEncoding() {
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeByte(0x50);
            buffer.writeByte(255); // Max ubyte value

            Object decoded = TypeDecoder.decode(buffer);
            assertTrue(decoded instanceof Number);
            assertEquals(255, ((Number) decoded).intValue() & 0xFF);
        }

        @Test
        @DisplayName("1.2.4: ushort - 16-bit unsigned (0x60)")
        void testUshortEncoding() {
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeByte(0x60);
            buffer.writeShort(65535); // Max ushort value

            Object decoded = TypeDecoder.decode(buffer);
            assertTrue(decoded instanceof Number);
            assertEquals(65535, ((Number) decoded).intValue() & 0xFFFF);
        }

        @Test
        @DisplayName("1.2.5: uint - 32-bit unsigned (0x70, 0x52, 0x43)")
        void testUintEncodings() {
            // Full encoding 0x70
            ByteBuf full = Unpooled.buffer();
            full.writeByte(0x70);
            full.writeInt(Integer.MAX_VALUE);
            Object decoded = TypeDecoder.decode(full);
            assertEquals(Integer.MAX_VALUE, ((Number) decoded).intValue());

            // Small uint 0x52 (single byte)
            ByteBuf small = Unpooled.buffer();
            small.writeByte(0x52);
            small.writeByte(200);
            decoded = TypeDecoder.decode(small);
            assertEquals(200, ((Number) decoded).intValue());

            // Zero 0x43
            ByteBuf zero = Unpooled.buffer();
            zero.writeByte(0x43);
            decoded = TypeDecoder.decode(zero);
            assertEquals(0, ((Number) decoded).intValue());
        }

        @Test
        @DisplayName("1.2.6: ulong - 64-bit unsigned (0x80, 0x53, 0x44)")
        void testUlongEncodings() {
            // Full encoding 0x80
            ByteBuf full = Unpooled.buffer();
            full.writeByte(0x80);
            full.writeLong(Long.MAX_VALUE);
            Object decoded = TypeDecoder.decode(full);
            assertEquals(Long.MAX_VALUE, ((Number) decoded).longValue());

            // Small ulong 0x53
            ByteBuf small = Unpooled.buffer();
            small.writeByte(0x53);
            small.writeByte(100);
            decoded = TypeDecoder.decode(small);
            assertEquals(100L, ((Number) decoded).longValue());

            // Zero 0x44
            ByteBuf zero = Unpooled.buffer();
            zero.writeByte(0x44);
            decoded = TypeDecoder.decode(zero);
            assertEquals(0L, ((Number) decoded).longValue());
        }

        @Test
        @DisplayName("1.2.7: byte - 8-bit signed (0x51)")
        void testByteEncoding() {
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeByte(0x51);
            buffer.writeByte(-128);

            Object decoded = TypeDecoder.decode(buffer);
            assertEquals((byte) -128, ((Number) decoded).byteValue());
        }

        @Test
        @DisplayName("1.2.8: short - 16-bit signed (0x61)")
        void testShortEncoding() {
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeByte(0x61);
            buffer.writeShort(-32768);

            Object decoded = TypeDecoder.decode(buffer);
            assertEquals((short) -32768, ((Number) decoded).shortValue());
        }

        @Test
        @DisplayName("1.2.9: int - 32-bit signed (0x71, 0x54)")
        void testIntEncodings() {
            // Full encoding 0x71
            ByteBuf full = Unpooled.buffer();
            full.writeByte(0x71);
            full.writeInt(-2147483648);
            Object decoded = TypeDecoder.decode(full);
            assertEquals(-2147483648, ((Number) decoded).intValue());

            // Small int 0x54
            ByteBuf small = Unpooled.buffer();
            small.writeByte(0x54);
            small.writeByte(-100);
            decoded = TypeDecoder.decode(small);
            assertEquals(-100, ((Number) decoded).intValue());
        }

        @Test
        @DisplayName("1.2.10: long - 64-bit signed (0x81, 0x55)")
        void testLongEncodings() {
            // Full encoding 0x81
            ByteBuf full = Unpooled.buffer();
            full.writeByte(0x81);
            full.writeLong(Long.MIN_VALUE);
            Object decoded = TypeDecoder.decode(full);
            assertEquals(Long.MIN_VALUE, ((Number) decoded).longValue());

            // Small long 0x55
            ByteBuf small = Unpooled.buffer();
            small.writeByte(0x55);
            small.writeByte(-50);
            decoded = TypeDecoder.decode(small);
            assertEquals(-50L, ((Number) decoded).longValue());
        }

        @Test
        @DisplayName("1.2.11-12: float/double IEEE 754")
        void testFloatingPointEncodings() {
            // float 0x72
            ByteBuf floatBuf = Unpooled.buffer();
            floatBuf.writeByte(0x72);
            floatBuf.writeFloat(3.14159f);
            Object decoded = TypeDecoder.decode(floatBuf);
            assertEquals(3.14159f, ((Number) decoded).floatValue(), 0.00001f);

            // double 0x82
            ByteBuf doubleBuf = Unpooled.buffer();
            doubleBuf.writeByte(0x82);
            doubleBuf.writeDouble(3.141592653589793);
            decoded = TypeDecoder.decode(doubleBuf);
            assertEquals(3.141592653589793, ((Number) decoded).doubleValue(), 0.0000001);
        }

        @Test
        @DisplayName("1.2.19: timestamp - 64-bit milliseconds since epoch (0x83)")
        void testTimestampEncoding() {
            long timestamp = System.currentTimeMillis();
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeByte(0x83);
            buffer.writeLong(timestamp);

            Object decoded = TypeDecoder.decode(buffer);
            // Decoder may return Date or Long - both are valid representations
            if (decoded instanceof java.util.Date) {
                assertEquals(timestamp, ((java.util.Date) decoded).getTime());
            } else {
                assertEquals(timestamp, ((Number) decoded).longValue());
            }
        }

        @Test
        @DisplayName("1.2.20: uuid - 128-bit UUID (0x98)")
        void testUuidEncoding() {
            UUID uuid = UUID.randomUUID();
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeByte(0x98);
            buffer.writeLong(uuid.getMostSignificantBits());
            buffer.writeLong(uuid.getLeastSignificantBits());

            Object decoded = TypeDecoder.decode(buffer);
            assertEquals(uuid, decoded);
        }
    }

    @Nested
    @DisplayName("Section 1.3 - Variable Width Types")
    class VariableWidthTypesCompliance {

        @Test
        @DisplayName("1.3.1: binary - variable-length bytes (0xa0/0xb0)")
        void testBinaryEncodings() {
            byte[] data = {1, 2, 3, 4, 5};

            // vbin8 (0xa0) - size <= 255
            ByteBuf vbin8 = Unpooled.buffer();
            vbin8.writeByte(0xa0);
            vbin8.writeByte(data.length);
            vbin8.writeBytes(data);
            Object decoded = TypeDecoder.decode(vbin8);
            assertTrue(decoded instanceof byte[]);
            assertArrayEquals(data, (byte[]) decoded);

            // vbin32 (0xb0)
            ByteBuf vbin32 = Unpooled.buffer();
            vbin32.writeByte(0xb0);
            vbin32.writeInt(data.length);
            vbin32.writeBytes(data);
            decoded = TypeDecoder.decode(vbin32);
            assertTrue(decoded instanceof byte[]);
            assertArrayEquals(data, (byte[]) decoded);
        }

        @Test
        @DisplayName("1.3.2: string - UTF-8 encoded (0xa1/0xb1)")
        void testStringEncodings() {
            String text = "Hello AMQP 1.0!";
            byte[] utf8 = text.getBytes(StandardCharsets.UTF_8);

            // str8 (0xa1)
            ByteBuf str8 = Unpooled.buffer();
            str8.writeByte(0xa1);
            str8.writeByte(utf8.length);
            str8.writeBytes(utf8);
            Object decoded = TypeDecoder.decode(str8);
            assertEquals(text, decoded);

            // str32 (0xb1)
            ByteBuf str32 = Unpooled.buffer();
            str32.writeByte(0xb1);
            str32.writeInt(utf8.length);
            str32.writeBytes(utf8);
            decoded = TypeDecoder.decode(str32);
            assertEquals(text, decoded);
        }

        @Test
        @DisplayName("1.3.3: symbol - ASCII encoded (0xa3/0xb3)")
        void testSymbolEncodings() {
            String symbolValue = "amqp:error:not-found";
            byte[] ascii = symbolValue.getBytes(StandardCharsets.US_ASCII);

            // sym8 (0xa3)
            ByteBuf sym8 = Unpooled.buffer();
            sym8.writeByte(0xa3);
            sym8.writeByte(ascii.length);
            sym8.writeBytes(ascii);
            Object decoded = TypeDecoder.decode(sym8);
            assertTrue(decoded instanceof Symbol);
            assertEquals(symbolValue, decoded.toString());

            // sym32 (0xb3)
            ByteBuf sym32 = Unpooled.buffer();
            sym32.writeByte(0xb3);
            sym32.writeInt(ascii.length);
            sym32.writeBytes(ascii);
            decoded = TypeDecoder.decode(sym32);
            assertTrue(decoded instanceof Symbol);
            assertEquals(symbolValue, decoded.toString());
        }
    }

    @Nested
    @DisplayName("Section 1.4 - Compound Types")
    class CompoundTypesCompliance {

        @Test
        @DisplayName("1.4.1: list - ordered sequence (0x45/0xc0/0xd0)")
        void testListEncodings() {
            // list0 (0x45) - empty list
            ByteBuf list0 = Unpooled.buffer();
            list0.writeByte(0x45);
            Object decoded = TypeDecoder.decode(list0);
            assertTrue(decoded instanceof List);
            assertTrue(((List<?>) decoded).isEmpty());
        }

        @Test
        @DisplayName("1.4.2: map - key-value pairs (0xc1/0xd1)")
        void testMapEncodings() {
            // map8 (0xc1)
            ByteBuf map8 = Unpooled.buffer();
            map8.writeByte(0xc1);
            int sizePos = map8.writerIndex();
            map8.writeByte(0); // placeholder for size
            map8.writeByte(2); // count = 2 (1 key-value pair = 2 items)
            int start = map8.writerIndex();
            // Key: "key"
            map8.writeByte(0xa1);
            map8.writeByte(3);
            map8.writeBytes("key".getBytes(StandardCharsets.UTF_8));
            // Value: true
            map8.writeByte(0x41);
            int size = map8.writerIndex() - start;
            map8.setByte(sizePos, size + 1);

            Object decoded = TypeDecoder.decode(map8);
            assertTrue(decoded instanceof Map);
            Map<?, ?> resultMap = (Map<?, ?>) decoded;
            assertEquals(1, resultMap.size());
            assertEquals(Boolean.TRUE, resultMap.get("key"));
        }
    }

    @Nested
    @DisplayName("Section 1.5 - Described Types")
    class DescribedTypesCompliance {

        @Test
        @DisplayName("1.5: described type - descriptor + value (0x00)")
        void testDescribedTypeEncoding() {
            // Described type with numeric descriptor
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeByte(0x00); // described type constructor
            // Descriptor: smallulong 0x10 (Open performative)
            buffer.writeByte(0x53);
            buffer.writeByte(0x10);
            // Value: empty list
            buffer.writeByte(0x45);

            Object decoded = TypeDecoder.decode(buffer);
            assertTrue(decoded instanceof DescribedType);
            DescribedType dt = (DescribedType) decoded;
            assertEquals(0x10L, ((Number) dt.getDescriptor()).longValue());
        }

        @Test
        @DisplayName("1.5: described type with symbol descriptor")
        void testDescribedTypeWithSymbolDescriptor() {
            String symbolName = "amqp:test";
            byte[] symbolBytes = symbolName.getBytes(StandardCharsets.US_ASCII);

            ByteBuf buffer = Unpooled.buffer();
            buffer.writeByte(0x00); // described type constructor
            // Descriptor: symbol
            buffer.writeByte(0xa3);
            buffer.writeByte(symbolBytes.length);
            buffer.writeBytes(symbolBytes);
            // Value: null
            buffer.writeByte(0x40);

            Object decoded = TypeDecoder.decode(buffer);
            assertTrue(decoded instanceof DescribedType);
            DescribedType dt = (DescribedType) decoded;
            assertTrue(dt.getDescriptor() instanceof Symbol);
            assertEquals(symbolName, dt.getDescriptor().toString());
        }
    }

    @Nested
    @DisplayName("Format Code Constants Validation")
    class FormatCodeValidation {

        @Test
        @DisplayName("Type format codes must match AMQP 1.0 specification")
        void testFormatCodeAssignments() {
            // Verify format codes match spec Table 1-1
            assertEquals((byte) 0x40, AmqpType.FormatCode.NULL);
            assertEquals((byte) 0x41, AmqpType.FormatCode.BOOLEAN_TRUE);
            assertEquals((byte) 0x42, AmqpType.FormatCode.BOOLEAN_FALSE);
            assertEquals((byte) 0x56, AmqpType.FormatCode.BOOLEAN);
            assertEquals((byte) 0x50, AmqpType.FormatCode.UBYTE);
            assertEquals((byte) 0x60, AmqpType.FormatCode.USHORT);
            assertEquals((byte) 0x70, AmqpType.FormatCode.UINT);
            assertEquals((byte) 0x80, AmqpType.FormatCode.ULONG);
            assertEquals((byte) 0x51, AmqpType.FormatCode.BYTE);
            assertEquals((byte) 0x61, AmqpType.FormatCode.SHORT);
            assertEquals((byte) 0x71, AmqpType.FormatCode.INT);
            assertEquals((byte) 0x81, AmqpType.FormatCode.LONG);
            assertEquals((byte) 0x72, AmqpType.FormatCode.FLOAT);
            assertEquals((byte) 0x82, AmqpType.FormatCode.DOUBLE);
            assertEquals((byte) 0x83, AmqpType.FormatCode.TIMESTAMP);
            assertEquals((byte) 0x98, AmqpType.FormatCode.UUID);
            assertEquals((byte) 0xa0, AmqpType.FormatCode.VBIN8);
            assertEquals((byte) 0xb0, AmqpType.FormatCode.VBIN32);
            assertEquals((byte) 0xa1, AmqpType.FormatCode.STR8_UTF8);
            assertEquals((byte) 0xb1, AmqpType.FormatCode.STR32_UTF8);
            assertEquals((byte) 0xa3, AmqpType.FormatCode.SYM8);
            assertEquals((byte) 0xb3, AmqpType.FormatCode.SYM32);
            assertEquals((byte) 0xc0, AmqpType.FormatCode.LIST8);
            assertEquals((byte) 0xd0, AmqpType.FormatCode.LIST32);
            assertEquals((byte) 0xc1, AmqpType.FormatCode.MAP8);
            assertEquals((byte) 0xd1, AmqpType.FormatCode.MAP32);
        }

        @Test
        @DisplayName("Performative descriptors must match specification")
        void testPerformativeDescriptors() {
            // Verify performative descriptor codes match spec
            assertEquals(0x10L, AmqpType.Descriptor.OPEN);
            assertEquals(0x11L, AmqpType.Descriptor.BEGIN);
            assertEquals(0x12L, AmqpType.Descriptor.ATTACH);
            assertEquals(0x13L, AmqpType.Descriptor.FLOW);
            assertEquals(0x14L, AmqpType.Descriptor.TRANSFER);
            assertEquals(0x15L, AmqpType.Descriptor.DISPOSITION);
            assertEquals(0x16L, AmqpType.Descriptor.DETACH);
            assertEquals(0x17L, AmqpType.Descriptor.END);
            assertEquals(0x18L, AmqpType.Descriptor.CLOSE);
        }

        @Test
        @DisplayName("SASL descriptors must match specification")
        void testSaslDescriptors() {
            assertEquals(0x40L, AmqpType.Descriptor.SASL_MECHANISMS);
            assertEquals(0x41L, AmqpType.Descriptor.SASL_INIT);
            assertEquals(0x42L, AmqpType.Descriptor.SASL_CHALLENGE);
            assertEquals(0x43L, AmqpType.Descriptor.SASL_RESPONSE);
            assertEquals(0x44L, AmqpType.Descriptor.SASL_OUTCOME);
        }

        @Test
        @DisplayName("Message section descriptors must match specification")
        void testMessageSectionDescriptors() {
            assertEquals(0x70L, AmqpType.Descriptor.HEADER);
            assertEquals(0x71L, AmqpType.Descriptor.DELIVERY_ANNOTATIONS);
            assertEquals(0x72L, AmqpType.Descriptor.MESSAGE_ANNOTATIONS);
            assertEquals(0x73L, AmqpType.Descriptor.PROPERTIES);
            assertEquals(0x74L, AmqpType.Descriptor.APPLICATION_PROPERTIES);
            assertEquals(0x75L, AmqpType.Descriptor.DATA);
            assertEquals(0x76L, AmqpType.Descriptor.AMQP_SEQUENCE);
            assertEquals(0x77L, AmqpType.Descriptor.AMQP_VALUE);
            assertEquals(0x78L, AmqpType.Descriptor.FOOTER);
        }

        @Test
        @DisplayName("Delivery state descriptors must match specification")
        void testDeliveryStateDescriptors() {
            assertEquals(0x23L, AmqpType.Descriptor.RECEIVED);
            assertEquals(0x24L, AmqpType.Descriptor.ACCEPTED);
            assertEquals(0x25L, AmqpType.Descriptor.REJECTED);
            assertEquals(0x26L, AmqpType.Descriptor.RELEASED);
            assertEquals(0x27L, AmqpType.Descriptor.MODIFIED);
        }

        @Test
        @DisplayName("Transaction descriptors must match specification")
        void testTransactionDescriptors() {
            assertEquals(0x30L, AmqpType.Descriptor.COORDINATOR);
            assertEquals(0x31L, AmqpType.Descriptor.DECLARE);
            assertEquals(0x32L, AmqpType.Descriptor.DISCHARGE);
            assertEquals(0x33L, AmqpType.Descriptor.DECLARED);
            assertEquals(0x34L, AmqpType.Descriptor.TRANSACTIONAL_STATE);
        }
    }
}
