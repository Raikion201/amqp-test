package com.amqp.protocol.v10.delivery;

import com.amqp.protocol.v10.transaction.Declared;
import com.amqp.protocol.v10.transaction.TransactionalState;
import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;

/**
 * Decoder for AMQP 1.0 delivery states.
 *
 * Decodes described types into their appropriate DeliveryState implementations.
 */
public class DeliveryStateDecoder {

    /**
     * Decode a described type into a DeliveryState.
     *
     * @param described the described type to decode
     * @return the decoded delivery state, or null if not a delivery state
     */
    public static DeliveryState decode(DescribedType described) {
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

        if (code == AmqpType.Descriptor.ACCEPTED) {
            return Accepted.decode(described);
        } else if (code == AmqpType.Descriptor.REJECTED) {
            return Rejected.decode(described);
        } else if (code == AmqpType.Descriptor.RELEASED) {
            return Released.decode(described);
        } else if (code == AmqpType.Descriptor.MODIFIED) {
            return Modified.decode(described);
        } else if (code == AmqpType.Descriptor.RECEIVED) {
            return Received.decode(described);
        } else if (code == AmqpType.Descriptor.DECLARED) {
            return Declared.decode(described);
        } else if (code == AmqpType.Descriptor.TRANSACTIONAL_STATE) {
            return TransactionalState.decode(described);
        }

        return null;
    }

    /**
     * Decode an object that might be a delivery state.
     *
     * @param obj the object to decode
     * @return the decoded delivery state, or null if not a delivery state
     */
    public static DeliveryState decodeIfDeliveryState(Object obj) {
        if (obj instanceof DeliveryState) {
            return (DeliveryState) obj;
        } else if (obj instanceof DescribedType) {
            return decode((DescribedType) obj);
        }
        return null;
    }
}
