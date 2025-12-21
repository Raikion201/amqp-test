package com.amqp.protocol.v10.delivery;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * AMQP 1.0 Modified delivery state.
 *
 * Indicates the message was modified and should be redelivered with changes.
 * Used to signal partial processing or request redelivery with modifications.
 *
 * Fields:
 * 0: delivery-failed (boolean) - Delivery count should be incremented
 * 1: undeliverable-here (boolean) - Don't redeliver to this link
 * 2: message-annotations (map) - Annotations to add to message
 */
public class Modified implements DeliveryState {

    public static final long DESCRIPTOR = AmqpType.Descriptor.MODIFIED;

    private Boolean deliveryFailed;
    private Boolean undeliverableHere;
    private Map<Symbol, Object> messageAnnotations;

    public Modified() {
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(deliveryFailed);
        fields.add(undeliverableHere);
        fields.add(messageAnnotations);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    @Override
    public boolean isTerminal() {
        return true;
    }

    // Getters
    public Boolean getDeliveryFailed() {
        return deliveryFailed;
    }

    public boolean isDeliveryFailed() {
        return deliveryFailed != null && deliveryFailed;
    }

    public Boolean getUndeliverableHere() {
        return undeliverableHere;
    }

    public boolean isUndeliverableHere() {
        return undeliverableHere != null && undeliverableHere;
    }

    public Map<Symbol, Object> getMessageAnnotations() {
        return messageAnnotations;
    }

    // Setters
    public Modified setDeliveryFailed(Boolean deliveryFailed) {
        this.deliveryFailed = deliveryFailed;
        return this;
    }

    public Modified setUndeliverableHere(Boolean undeliverableHere) {
        this.undeliverableHere = undeliverableHere;
        return this;
    }

    public Modified setMessageAnnotations(Map<Symbol, Object> messageAnnotations) {
        this.messageAnnotations = messageAnnotations;
        return this;
    }

    @SuppressWarnings("unchecked")
    public static Modified decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        Modified modified = new Modified();

        modified.deliveryFailed = TypeDecoder.getField(fields, 0, Boolean.class);
        modified.undeliverableHere = TypeDecoder.getField(fields, 1, Boolean.class);

        Object annotationsField = TypeDecoder.getField(fields, 2);
        if (annotationsField instanceof Map) {
            modified.messageAnnotations = (Map<Symbol, Object>) annotationsField;
        }

        return modified;
    }

    @Override
    public String toString() {
        return String.format("Modified{deliveryFailed=%s, undeliverableHere=%s}",
                deliveryFailed, undeliverableHere);
    }
}
