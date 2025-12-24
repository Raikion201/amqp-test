package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;
import com.amqp.protocol.v10.types.UInt;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 Disposition performative.
 *
 * Sent to inform about delivery state changes.
 *
 * Fields:
 * 0: role (role, mandatory) - Role of sender (false=sender, true=receiver)
 * 1: first (delivery-number, mandatory) - First delivery-id in range
 * 2: last (delivery-number) - Last delivery-id in range
 * 3: settled (boolean) - If true, deliveries are settled
 * 4: state (delivery-state) - New state for deliveries
 * 5: batchable (boolean) - Can be batched
 */
public class Disposition implements Performative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.DISPOSITION;

    private final boolean role; // false = sender, true = receiver
    private final long first;
    private Long last;
    private Boolean settled;
    private Object state; // DeliveryState
    private Boolean batchable;

    public Disposition(boolean role, long first) {
        this.role = role;
        this.first = first;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public List<Object> getFields() {
        List<Object> fields = new ArrayList<>();
        fields.add(role);
        // first is delivery-number (uint) per AMQP 1.0 spec
        fields.add(UInt.valueOf(first));
        // last is delivery-number (uint) per AMQP 1.0 spec
        fields.add(last == null ? null : UInt.valueOf(last));
        fields.add(settled);
        fields.add(state);
        fields.add(batchable);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return fields;
    }

    // Getters
    public boolean getRole() {
        return role;
    }

    public boolean isSender() {
        return !role;
    }

    public boolean isReceiver() {
        return role;
    }

    public long getFirst() {
        return first;
    }

    public Long getLast() {
        return last;
    }

    public long getLastOrFirst() {
        return last != null ? last : first;
    }

    public Boolean getSettled() {
        return settled;
    }

    public boolean isSettled() {
        return settled != null && settled;
    }

    public Object getState() {
        return state;
    }

    public Boolean getBatchable() {
        return batchable;
    }

    public boolean isBatchable() {
        return batchable != null && batchable;
    }

    // Setters
    public Disposition setLast(Long last) {
        this.last = last;
        return this;
    }

    public Disposition setSettled(Boolean settled) {
        this.settled = settled;
        return this;
    }

    public Disposition setState(Object state) {
        this.state = state;
        return this;
    }

    public Disposition setBatchable(Boolean batchable) {
        this.batchable = batchable;
        return this;
    }

    /**
     * Check if a delivery ID is within the disposition range.
     */
    public boolean covers(long deliveryId) {
        long end = last != null ? last : first;
        return deliveryId >= first && deliveryId <= end;
    }

    public static Disposition decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);

        boolean role = TypeDecoder.getBooleanField(fields, 0, false);
        long first = TypeDecoder.getLongField(fields, 1, 0);

        Disposition disposition = new Disposition(role, first);

        Object last = TypeDecoder.getField(fields, 2);
        if (last instanceof Number) {
            disposition.last = ((Number) last).longValue();
        }

        disposition.settled = TypeDecoder.getField(fields, 3, Boolean.class);
        disposition.state = TypeDecoder.getField(fields, 4);
        disposition.batchable = TypeDecoder.getField(fields, 5, Boolean.class);

        return disposition;
    }

    @Override
    public String toString() {
        return String.format("Disposition{role=%s, first=%d, last=%s, settled=%s, state=%s}",
                role ? "receiver" : "sender", first, last, settled, state);
    }
}
