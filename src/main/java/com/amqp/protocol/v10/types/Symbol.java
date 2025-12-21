package com.amqp.protocol.v10.types;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * AMQP 1.0 Symbol type.
 * Symbols are ASCII strings used for identifiers and keys.
 */
public final class Symbol implements Comparable<Symbol> {

    private final String value;

    private Symbol(String value) {
        this.value = value;
    }

    public static Symbol valueOf(String value) {
        if (value == null) {
            return null;
        }
        return new Symbol(value);
    }

    public static Symbol getSymbol(String value) {
        return valueOf(value);
    }

    public byte[] getBytes() {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    public int length() {
        return value.length();
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Symbol symbol = (Symbol) obj;
        return Objects.equals(value, symbol.value);
    }

    @Override
    public int compareTo(Symbol other) {
        return value.compareTo(other.value);
    }
}
