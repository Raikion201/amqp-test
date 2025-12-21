package com.amqp.protocol.v10.delivery;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;

import java.util.Collections;

/**
 * AMQP 1.0 Accepted delivery state.
 *
 * Indicates the message was successfully processed.
 * This is the most common outcome for acknowledged messages.
 */
public class Accepted implements DeliveryState {

    public static final long DESCRIPTOR = AmqpType.Descriptor.ACCEPTED;

    // Singleton instance since Accepted has no fields
    public static final Accepted INSTANCE = new Accepted();

    public Accepted() {
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public DescribedType toDescribed() {
        return new DescribedType.Default(DESCRIPTOR, Collections.emptyList());
    }

    @Override
    public boolean isTerminal() {
        return true;
    }

    public static Accepted decode(DescribedType described) {
        return INSTANCE;
    }

    @Override
    public String toString() {
        return "Accepted{}";
    }
}
