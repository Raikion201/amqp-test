package com.amqp.protocol.v10.types;

import java.math.BigInteger;

/**
 * AMQP 1.0 Unsigned Long type.
 * Represents an unsigned 64-bit integer (0 to 2^64 - 1).
 *
 * This class is used to preserve type information for AMQP 1.0 message-id
 * and correlation-id fields, which can be ulong, uuid, binary, or string.
 */
public class ULong extends Number implements Comparable<ULong> {
    private static final long serialVersionUID = 1L;

    public static final ULong ZERO = new ULong(0);

    private final long value;

    public ULong(long value) {
        this.value = value;
    }

    public static ULong valueOf(long value) {
        if (value == 0) {
            return ZERO;
        }
        return new ULong(value);
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
        return (float) bigIntegerValue().floatValue();
    }

    @Override
    public double doubleValue() {
        return bigIntegerValue().doubleValue();
    }

    public BigInteger bigIntegerValue() {
        if (value >= 0) {
            return BigInteger.valueOf(value);
        }
        return BigInteger.valueOf(value & 0x7FFFFFFFFFFFFFFFL)
                         .add(BigInteger.valueOf(Long.MAX_VALUE))
                         .add(BigInteger.ONE);
    }

    @Override
    public int compareTo(ULong other) {
        return Long.compareUnsigned(this.value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;

        // Also compare with Proton's UnsignedLong by class name
        if (obj.getClass().getSimpleName().equals("UnsignedLong")) {
            return value == ((Number) obj).longValue();
        }

        if (!(obj instanceof ULong)) return false;
        return value == ((ULong) obj).value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    @Override
    public String toString() {
        return bigIntegerValue().toString();
    }
}
