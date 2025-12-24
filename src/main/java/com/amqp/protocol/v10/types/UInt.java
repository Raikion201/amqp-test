package com.amqp.protocol.v10.types;

/**
 * Wrapper for AMQP 1.0 unsigned 32-bit integer values.
 *
 * This class ensures values are encoded with format code 0x70 (UINT),
 * 0x52 (SMALL_UINT), or 0x43 (UINT_0) rather than signed int encoding.
 */
public class UInt extends Number implements Comparable<UInt> {

    private static final long serialVersionUID = 1L;

    private final long value;

    public UInt(long value) {
        if (value < 0 || value > 0xFFFFFFFFL) {
            throw new IllegalArgumentException("UInt value must be 0-4294967295, got: " + value);
        }
        this.value = value;
    }

    public static UInt valueOf(long value) {
        return new UInt(value);
    }

    public static UInt valueOf(int value) {
        // Treat as unsigned
        return new UInt(Integer.toUnsignedLong(value));
    }

    @Override
    public int intValue() {
        return (int) value;
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
    public int compareTo(UInt other) {
        return Long.compare(this.value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof UInt)) return false;
        return this.value == ((UInt) obj).value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
