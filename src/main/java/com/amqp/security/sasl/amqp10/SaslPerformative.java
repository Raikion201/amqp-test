package com.amqp.security.sasl.amqp10;

import com.amqp.protocol.v10.types.DescribedType;

/**
 * Base interface for AMQP 1.0 SASL performatives.
 */
public interface SaslPerformative {

    /**
     * Get the descriptor code for this performative.
     */
    long getDescriptor();

    /**
     * Convert to a described type for encoding.
     */
    DescribedType toDescribed();
}
