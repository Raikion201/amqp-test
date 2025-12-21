package com.amqp.protocol.v10.delivery;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 Received delivery state.
 *
 * Indicates partial receipt of a message (used for resuming transfers).
 * This is a non-terminal state.
 *
 * Fields:
 * 0: section-number (uint) - Section number received
 * 1: section-offset (ulong) - Offset within section
 */
public class Received implements DeliveryState {

    public static final long DESCRIPTOR = AmqpType.Descriptor.RECEIVED;

    private long sectionNumber;
    private long sectionOffset;

    public Received() {
    }

    public Received(long sectionNumber, long sectionOffset) {
        this.sectionNumber = sectionNumber;
        this.sectionOffset = sectionOffset;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(sectionNumber);
        fields.add(sectionOffset);

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    @Override
    public boolean isTerminal() {
        return false; // Received is not terminal, transfer can resume
    }

    // Getters
    public long getSectionNumber() {
        return sectionNumber;
    }

    public long getSectionOffset() {
        return sectionOffset;
    }

    // Setters
    public Received setSectionNumber(long sectionNumber) {
        this.sectionNumber = sectionNumber;
        return this;
    }

    public Received setSectionOffset(long sectionOffset) {
        this.sectionOffset = sectionOffset;
        return this;
    }

    public static Received decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        Received received = new Received();

        received.sectionNumber = TypeDecoder.getLongField(fields, 0, 0);
        received.sectionOffset = TypeDecoder.getLongField(fields, 1, 0);

        return received;
    }

    @Override
    public String toString() {
        return String.format("Received{sectionNumber=%d, sectionOffset=%d}",
                sectionNumber, sectionOffset);
    }
}
