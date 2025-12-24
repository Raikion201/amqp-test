package com.amqp.protocol.v10.types;

/**
 * Wrapper for AMQP 1.0 unsigned 16-bit integer values.
 *
 * This class ensures values are encoded with format code 0x60 (USHORT)
 * rather than signed short encoding.
 */
public class UShort extends Number implements Comparable<UShort> {

    private static final long serialVersionUID = 1L;

    private final int value;

    public UShort(int value) {
        if (value < 0 || value > 65535) {
            throw new IllegalArgumentException("UShort value must be 0-65535, got: " + value);
        }
        this.value = value;
    }

    public static UShort valueOf(int value) {
        return new UShort(value);
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
    public short shortValue() {
        return (short) value;
    }

    @Override
    public int compareTo(UShort other) {
        return Integer.compare(this.value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof UShort)) return false;
        return this.value == ((UShort) obj).value;
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
