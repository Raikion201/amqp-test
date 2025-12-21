package com.amqp.protocol.v10.messaging;

import com.amqp.protocol.v10.types.DescribedType;

/**
 * Base interface for AMQP 1.0 message sections.
 *
 * An AMQP 1.0 message consists of multiple sections:
 * - Header (transport-related properties)
 * - DeliveryAnnotations (hop-by-hop annotations)
 * - MessageAnnotations (message-level annotations)
 * - Properties (application-level properties)
 * - ApplicationProperties (custom properties)
 * - Body (one or more Data, AmqpSequence, or AmqpValue sections)
 * - Footer (message verification)
 */
public interface MessageSection {

    /**
     * Get the descriptor code for this section.
     */
    long getDescriptor();

    /**
     * Get the type of this section.
     */
    SectionType getSectionType();

    /**
     * Convert to a described type for encoding.
     */
    DescribedType toDescribed();

    /**
     * Section types in a message.
     */
    enum SectionType {
        HEADER,
        DELIVERY_ANNOTATIONS,
        MESSAGE_ANNOTATIONS,
        PROPERTIES,
        APPLICATION_PROPERTIES,
        DATA,
        AMQP_SEQUENCE,
        AMQP_VALUE,
        FOOTER
    }
}
