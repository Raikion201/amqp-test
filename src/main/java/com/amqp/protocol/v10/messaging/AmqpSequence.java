package com.amqp.protocol.v10.messaging;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * AMQP 1.0 AmqpSequence body section.
 *
 * Contains a sequence of AMQP values. A message may have multiple
 * AmqpSequence sections.
 */
public class AmqpSequence implements MessageSection {

    public static final long DESCRIPTOR = AmqpType.Descriptor.AMQP_SEQUENCE;

    private List<Object> values;

    public AmqpSequence() {
        this.values = new ArrayList<>();
    }

    public AmqpSequence(List<Object> values) {
        this.values = values != null ? new ArrayList<>(values) : new ArrayList<>();
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public SectionType getSectionType() {
        return SectionType.AMQP_SEQUENCE;
    }

    @Override
    public DescribedType toDescribed() {
        return new DescribedType.Default(DESCRIPTOR, values);
    }

    public List<Object> getValue() {
        return values;
    }

    public Object get(int index) {
        return values.get(index);
    }

    public int size() {
        return values.size();
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public AmqpSequence add(Object value) {
        values.add(value);
        return this;
    }

    public AmqpSequence addAll(List<Object> values) {
        this.values.addAll(values);
        return this;
    }

    public AmqpSequence setValue(List<Object> values) {
        this.values = values != null ? new ArrayList<>(values) : new ArrayList<>();
        return this;
    }

    @SuppressWarnings("unchecked")
    public static AmqpSequence decode(DescribedType described) {
        Object value = described.getDescribed();
        if (value instanceof List) {
            return new AmqpSequence((List<Object>) value);
        }
        return new AmqpSequence();
    }

    @Override
    public String toString() {
        return String.format("AmqpSequence{size=%d}", values.size());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AmqpSequence that = (AmqpSequence) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }
}
