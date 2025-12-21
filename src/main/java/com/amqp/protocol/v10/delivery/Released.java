package com.amqp.protocol.v10.delivery;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;

import java.util.Collections;

/**
 * AMQP 1.0 Released delivery state.
 *
 * Indicates the message was not processed and should be redelivered
 * to another consumer (or the same consumer later).
 */
public class Released implements DeliveryState {

    public static final long DESCRIPTOR = AmqpType.Descriptor.RELEASED;

    // Singleton instance since Released has no fields
    public static final Released INSTANCE = new Released();

    public Released() {
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

    public static Released decode(DescribedType described) {
        return INSTANCE;
    }

    @Override
    public String toString() {
        return "Released{}";
    }
}
