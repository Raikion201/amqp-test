package com.amqp.protocol.v10.messaging;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;

/**
 * Decoder for AMQP 1.0 message sections.
 */
public class SectionDecoder {

    /**
     * Decode a described type into a MessageSection.
     *
     * @param described the described type to decode
     * @return the decoded section, or null if not a message section
     */
    public static MessageSection decode(DescribedType described) {
        if (described == null) {
            return null;
        }

        Object descriptor = described.getDescriptor();
        long code;

        if (descriptor instanceof Number) {
            code = ((Number) descriptor).longValue();
        } else {
            return null;
        }

        if (code == AmqpType.Descriptor.HEADER) {
            return Header.decode(described);
        } else if (code == AmqpType.Descriptor.DELIVERY_ANNOTATIONS) {
            return DeliveryAnnotations.decode(described);
        } else if (code == AmqpType.Descriptor.MESSAGE_ANNOTATIONS) {
            return MessageAnnotations.decode(described);
        } else if (code == AmqpType.Descriptor.PROPERTIES) {
            return Properties.decode(described);
        } else if (code == AmqpType.Descriptor.APPLICATION_PROPERTIES) {
            return ApplicationProperties.decode(described);
        } else if (code == AmqpType.Descriptor.DATA) {
            return Data.decode(described);
        } else if (code == AmqpType.Descriptor.AMQP_VALUE) {
            return AmqpValue.decode(described);
        } else if (code == AmqpType.Descriptor.AMQP_SEQUENCE) {
            return AmqpSequence.decode(described);
        } else if (code == AmqpType.Descriptor.FOOTER) {
            return Footer.decode(described);
        }

        return null;
    }

    /**
     * Check if a descriptor code represents a body section.
     */
    public static boolean isBodySection(long descriptorCode) {
        return descriptorCode == AmqpType.Descriptor.DATA
                || descriptorCode == AmqpType.Descriptor.AMQP_VALUE
                || descriptorCode == AmqpType.Descriptor.AMQP_SEQUENCE;
    }

    /**
     * Get the section type for a descriptor code.
     */
    public static MessageSection.SectionType getSectionType(long descriptorCode) {
        if (descriptorCode == AmqpType.Descriptor.HEADER) {
            return MessageSection.SectionType.HEADER;
        } else if (descriptorCode == AmqpType.Descriptor.DELIVERY_ANNOTATIONS) {
            return MessageSection.SectionType.DELIVERY_ANNOTATIONS;
        } else if (descriptorCode == AmqpType.Descriptor.MESSAGE_ANNOTATIONS) {
            return MessageSection.SectionType.MESSAGE_ANNOTATIONS;
        } else if (descriptorCode == AmqpType.Descriptor.PROPERTIES) {
            return MessageSection.SectionType.PROPERTIES;
        } else if (descriptorCode == AmqpType.Descriptor.APPLICATION_PROPERTIES) {
            return MessageSection.SectionType.APPLICATION_PROPERTIES;
        } else if (descriptorCode == AmqpType.Descriptor.DATA) {
            return MessageSection.SectionType.DATA;
        } else if (descriptorCode == AmqpType.Descriptor.AMQP_VALUE) {
            return MessageSection.SectionType.AMQP_VALUE;
        } else if (descriptorCode == AmqpType.Descriptor.AMQP_SEQUENCE) {
            return MessageSection.SectionType.AMQP_SEQUENCE;
        } else if (descriptorCode == AmqpType.Descriptor.FOOTER) {
            return MessageSection.SectionType.FOOTER;
        }
        return null;
    }
}
