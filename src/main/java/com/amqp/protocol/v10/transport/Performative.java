package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.TypeEncoder;
import io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * Base interface for AMQP 1.0 transport performatives.
 *
 * Performatives are the protocol messages exchanged between AMQP peers.
 * Each performative is encoded as a described list.
 */
public interface Performative {

    /**
     * Get the descriptor code for this performative.
     */
    long getDescriptor();

    /**
     * Get the fields of this performative as a list.
     */
    List<Object> getFields();

    /**
     * Encode this performative to a ByteBuf.
     */
    default void encode(ByteBuf buffer) {
        TypeEncoder.encodeDescribedList(getDescriptor(), getFields(), buffer);
    }

    /**
     * Performative types.
     */
    enum Type {
        OPEN(AmqpType.Descriptor.OPEN),
        BEGIN(AmqpType.Descriptor.BEGIN),
        ATTACH(AmqpType.Descriptor.ATTACH),
        FLOW(AmqpType.Descriptor.FLOW),
        TRANSFER(AmqpType.Descriptor.TRANSFER),
        DISPOSITION(AmqpType.Descriptor.DISPOSITION),
        DETACH(AmqpType.Descriptor.DETACH),
        END(AmqpType.Descriptor.END),
        CLOSE(AmqpType.Descriptor.CLOSE);

        private final long descriptor;

        Type(long descriptor) {
            this.descriptor = descriptor;
        }

        public long getDescriptor() {
            return descriptor;
        }

        public static Type fromDescriptor(long descriptor) {
            for (Type type : values()) {
                if (type.descriptor == descriptor) {
                    return type;
                }
            }
            return null;
        }
    }
}
