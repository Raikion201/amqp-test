package com.amqp.protocol.v10.types;

/**
 * Wrapper for AMQP 1.0 unsigned byte values.
 *
 * This class ensures values are encoded with format code 0x50 (UBYTE)
 * rather than 0x54 (SMALL_INT) when passed to TypeEncoder.
 */
public class UByte extends Number implements Comparable<UByte> {

    private static final long serialVersionUID = 1L;

    private final int value;

    public UByte(int value) {
        if (value < 0 || value > 255) {
            throw new IllegalArgumentException("UByte value must be 0-255, got: " + value);
        }
        this.value = value;
    }

    public static UByte valueOf(int value) {
        return new UByte(value);
    }

    @Override
    public int intValue() {
        return value;
    }

    @Override
    public long longValue() {
        return value;
    }

    @Override
    public float floatValue() {
        return value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public byte byteValue() {
        return (byte) value;
    }

    @Override
    public int compareTo(UByte other) {
        return Integer.compare(this.value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof UByte)) return false;
        return this.value == ((UByte) obj).value;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
