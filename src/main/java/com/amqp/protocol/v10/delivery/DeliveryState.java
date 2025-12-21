package com.amqp.protocol.v10.delivery;

import com.amqp.protocol.v10.types.DescribedType;

/**
 * Base interface for AMQP 1.0 delivery states.
 *
 * Delivery states indicate the state of a message delivery.
 * They are used in Transfer and Disposition performatives.
 */
public interface DeliveryState {

    /**
     * Get the descriptor code for this delivery state.
     */
    long getDescriptor();

    /**
     * Convert to a described type for encoding.
     */
    DescribedType toDescribed();

    /**
     * Check if this is a terminal state (delivery is complete).
     */
    boolean isTerminal();
}
