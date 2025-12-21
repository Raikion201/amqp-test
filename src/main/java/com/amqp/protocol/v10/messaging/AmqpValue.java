package com.amqp.protocol.v10.messaging;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;

import java.util.Objects;

/**
 * AMQP 1.0 AmqpValue body section.
 *
 * Contains a single AMQP value. This is the most common body type
 * for simple messages.
 */
public class AmqpValue implements MessageSection {

    public static final long DESCRIPTOR = AmqpType.Descriptor.AMQP_VALUE;

    private Object value;

    public AmqpValue() {
    }

    public AmqpValue(Object value) {
        this.value = value;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public SectionType getSectionType() {
        return SectionType.AMQP_VALUE;
    }

    @Override
    public DescribedType toDescribed() {
        return new DescribedType.Default(DESCRIPTOR, value);
    }

    public Object getValue() {
        return value;
    }

    public String getValueAsString() {
        return value instanceof String ? (String) value : (value != null ? value.toString() : null);
    }

    @SuppressWarnings("unchecked")
    public <T> T getValueAs(Class<T> type) {
        if (value == null) {
            return null;
        }
        if (type.isInstance(value)) {
            return (T) value;
        }
        return null;
    }

    public AmqpValue setValue(Object value) {
        this.value = value;
        return this;
    }

    public static AmqpValue decode(DescribedType described) {
        return new AmqpValue(described.getDescribed());
    }

    @Override
    public String toString() {
        return String.format("AmqpValue{value=%s}", value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AmqpValue amqpValue = (AmqpValue) o;
        return Objects.equals(value, amqpValue.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
