package com.amqp.protocol.v10.types;

import io.netty.buffer.ByteBuf;

/**
 * Base interface for all AMQP 1.0 types.
 * AMQP 1.0 defines a rich type system with primitives, composites, and described types.
 */
public interface AmqpType {

    /**
     * Get the format code for this type.
     * Format codes identify the type on the wire.
     */
    byte getFormatCode();

    /**
     * Encode this type to a ByteBuf.
     */
    void encode(ByteBuf buffer);

    /**
     * Get the encoded size in bytes.
     */
    int getEncodedSize();

    /**
     * AMQP 1.0 format codes for primitive types.
     */
    interface FormatCode {
        // Described type constructor
        byte DESCRIBED = 0x00;

        // Fixed-width types (category 0x4x)
        byte NULL = 0x40;
        byte BOOLEAN_TRUE = 0x41;
        byte BOOLEAN_FALSE = 0x42;
        byte UINT_0 = 0x43;
        byte ULONG_0 = 0x44;
        byte LIST_0 = 0x45;

        // Fixed-width 1-byte (category 0x5x)
        byte UBYTE = 0x50;
        byte BYTE = 0x51;
        byte SMALL_UINT = 0x52;
        byte SMALL_ULONG = 0x53;
        byte SMALL_INT = 0x54;
        byte SMALL_LONG = 0x55;
        byte BOOLEAN = 0x56;

        // Fixed-width 2-byte (category 0x6x)
        byte USHORT = 0x60;
        byte SHORT = 0x61;

        // Fixed-width 4-byte (category 0x7x)
        byte UINT = 0x70;
        byte INT = 0x71;
        byte FLOAT = 0x72;
        byte CHAR = 0x73;
        byte DECIMAL32 = 0x74;

        // Fixed-width 8-byte (category 0x8x)
        byte ULONG = (byte) 0x80;
        byte LONG = (byte) 0x81;
        byte DOUBLE = (byte) 0x82;
        byte TIMESTAMP = (byte) 0x83;
        byte DECIMAL64 = (byte) 0x84;

        // Fixed-width 16-byte (category 0x9x)
        byte DECIMAL128 = (byte) 0x94;
        byte UUID = (byte) 0x98;

        // Variable-width 1-byte size (category 0xAx)
        byte VBIN8 = (byte) 0xA0;
        byte STR8_UTF8 = (byte) 0xA1;
        byte SYM8 = (byte) 0xA3;

        // Variable-width 4-byte size (category 0xBx)
        byte VBIN32 = (byte) 0xB0;
        byte STR32_UTF8 = (byte) 0xB1;
        byte SYM32 = (byte) 0xB3;

        // Compound types 1-byte count (category 0xCx)
        byte LIST8 = (byte) 0xC0;
        byte MAP8 = (byte) 0xC1;

        // Compound types 4-byte count (category 0xDx)
        byte LIST32 = (byte) 0xD0;
        byte MAP32 = (byte) 0xD1;

        // Array types (category 0xEx)
        byte ARRAY8 = (byte) 0xE0;
        byte ARRAY32 = (byte) 0xF0;
    }

    /**
     * Descriptor codes for AMQP 1.0 described types.
     * These are used for performatives and message sections.
     */
    interface Descriptor {
        // Transport performatives (0x00000000:0x00000010 - 0x00000000:0x00000018)
        long OPEN = 0x0000000000000010L;
        long BEGIN = 0x0000000000000011L;
        long ATTACH = 0x0000000000000012L;
        long FLOW = 0x0000000000000013L;
        long TRANSFER = 0x0000000000000014L;
        long DISPOSITION = 0x0000000000000015L;
        long DETACH = 0x0000000000000016L;
        long END = 0x0000000000000017L;
        long CLOSE = 0x0000000000000018L;

        // SASL performatives (0x00000000:0x00000040 - 0x00000000:0x00000044)
        long SASL_MECHANISMS = 0x0000000000000040L;
        long SASL_INIT = 0x0000000000000041L;
        long SASL_CHALLENGE = 0x0000000000000042L;
        long SASL_RESPONSE = 0x0000000000000043L;
        long SASL_OUTCOME = 0x0000000000000044L;

        // Message sections (0x00000000:0x00000070 - 0x00000000:0x00000078)
        long HEADER = 0x0000000000000070L;
        long DELIVERY_ANNOTATIONS = 0x0000000000000071L;
        long MESSAGE_ANNOTATIONS = 0x0000000000000072L;
        long PROPERTIES = 0x0000000000000073L;
        long APPLICATION_PROPERTIES = 0x0000000000000074L;
        long DATA = 0x0000000000000075L;
        long AMQP_SEQUENCE = 0x0000000000000076L;
        long AMQP_VALUE = 0x0000000000000077L;
        long FOOTER = 0x0000000000000078L;

        // Delivery states (0x00000000:0x00000023 - 0x00000000:0x00000027)
        long RECEIVED = 0x0000000000000023L;
        long ACCEPTED = 0x0000000000000024L;
        long REJECTED = 0x0000000000000025L;
        long RELEASED = 0x0000000000000026L;
        long MODIFIED = 0x0000000000000027L;

        // Source and Target (0x00000000:0x00000028 - 0x00000000:0x00000029)
        long SOURCE = 0x0000000000000028L;
        long TARGET = 0x0000000000000029L;

        // Error (0x00000000:0x0000001D)
        long ERROR = 0x000000000000001DL;

        // Transaction types (0x00000000:0x00000030 - 0x00000000:0x00000034)
        long COORDINATOR = 0x0000000000000030L;
        long DECLARE = 0x0000000000000031L;
        long DISCHARGE = 0x0000000000000032L;
        long DECLARED = 0x0000000000000033L;
        long TRANSACTIONAL_STATE = 0x0000000000000034L;
    }
}
