package com.amqp.protocol.v10.frame;

/**
 * AMQP 1.0 frame types.
 */
public enum FrameType {

    /**
     * AMQP frame (transport layer) - contains performatives.
     */
    AMQP((byte) 0x00),

    /**
     * SASL frame (security layer) - contains SASL mechanisms.
     */
    SASL((byte) 0x01);

    private final byte code;

    FrameType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static FrameType fromCode(byte code) {
        switch (code) {
            case 0x00:
                return AMQP;
            case 0x01:
                return SASL;
            default:
                throw new IllegalArgumentException("Unknown frame type: 0x" +
                    Integer.toHexString(code & 0xFF));
        }
    }
}
