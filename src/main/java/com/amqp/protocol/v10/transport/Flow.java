package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;
import com.amqp.protocol.v10.types.TypeDecoder;
import com.amqp.protocol.v10.types.UInt;

import java.util.*;

/**
 * AMQP 1.0 Flow performative.
 *
 * Sent to update flow state for session or link.
 *
 * Fields:
 * 0: next-incoming-id (transfer-number) - Expected next transfer-id
 * 1: incoming-window (uint, mandatory) - Incoming window size
 * 2: next-outgoing-id (transfer-number, mandatory) - Next outgoing transfer-id
 * 3: outgoing-window (uint, mandatory) - Outgoing window size
 * 4: handle (handle) - Link handle (if link-specific flow)
 * 5: delivery-count (sequence-no) - Delivery count at sender
 * 6: link-credit (uint) - Link credit
 * 7: available (uint) - Messages available
 * 8: drain (boolean) - Drain mode
 * 9: echo (boolean) - Request flow state echo
 * 10: properties (map) - Link state properties
 */
public class Flow implements Performative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.FLOW;

    private Long nextIncomingId;
    private long incomingWindow;
    private long nextOutgoingId;
    private long outgoingWindow;
    private Long handle;
    private Long deliveryCount;
    private Long linkCredit;
    private Long available;
    private Boolean drain;
    private Boolean echo;
    private Map<Symbol, Object> properties;

    public Flow(long incomingWindow, long nextOutgoingId, long outgoingWindow) {
        this.incomingWindow = incomingWindow;
        this.nextOutgoingId = nextOutgoingId;
        this.outgoingWindow = outgoingWindow;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public List<Object> getFields() {
        List<Object> fields = new ArrayList<>();
        // next-incoming-id is transfer-number (uint) per AMQP 1.0 spec
        fields.add(nextIncomingId == null ? null : UInt.valueOf(nextIncomingId));
        // incoming-window is uint per AMQP 1.0 spec
        fields.add(UInt.valueOf(incomingWindow));
        // next-outgoing-id is transfer-number (uint) per AMQP 1.0 spec
        fields.add(UInt.valueOf(nextOutgoingId));
        // outgoing-window is uint per AMQP 1.0 spec
        fields.add(UInt.valueOf(outgoingWindow));
        // handle is uint per AMQP 1.0 spec
        fields.add(handle == null ? null : UInt.valueOf(handle));
        // delivery-count is sequence-no (uint) per AMQP 1.0 spec
        fields.add(deliveryCount == null ? null : UInt.valueOf(deliveryCount));
        // link-credit is uint per AMQP 1.0 spec
        fields.add(linkCredit == null ? null : UInt.valueOf(linkCredit));
        // available is uint per AMQP 1.0 spec
        fields.add(available == null ? null : UInt.valueOf(available));
        fields.add(drain);
        fields.add(echo);
        fields.add(properties);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return fields;
    }

    // Getters
    public Long getNextIncomingId() {
        return nextIncomingId;
    }

    public long getIncomingWindow() {
        return incomingWindow;
    }

    public long getNextOutgoingId() {
        return nextOutgoingId;
    }

    public long getOutgoingWindow() {
        return outgoingWindow;
    }

    public Long getHandle() {
        return handle;
    }

    public Long getDeliveryCount() {
        return deliveryCount;
    }

    public Long getLinkCredit() {
        return linkCredit;
    }

    public Long getAvailable() {
        return available;
    }

    public Boolean getDrain() {
        return drain;
    }

    public boolean isDrain() {
        return drain != null && drain;
    }

    public Boolean getEcho() {
        return echo;
    }

    public boolean isEcho() {
        return echo != null && echo;
    }

    public Map<Symbol, Object> getProperties() {
        return properties;
    }

    public boolean isLinkFlow() {
        return handle != null;
    }

    // Setters
    public Flow setNextIncomingId(Long nextIncomingId) {
        this.nextIncomingId = nextIncomingId;
        return this;
    }

    public Flow setIncomingWindow(long incomingWindow) {
        this.incomingWindow = incomingWindow;
        return this;
    }

    public Flow setHandle(Long handle) {
        this.handle = handle;
        return this;
    }

    public Flow setDeliveryCount(Long deliveryCount) {
        this.deliveryCount = deliveryCount;
        return this;
    }

    public Flow setLinkCredit(Long linkCredit) {
        this.linkCredit = linkCredit;
        return this;
    }

    public Flow setAvailable(Long available) {
        this.available = available;
        return this;
    }

    public Flow setDrain(Boolean drain) {
        this.drain = drain;
        return this;
    }

    public Flow setEcho(Boolean echo) {
        this.echo = echo;
        return this;
    }

    public Flow setProperties(Map<Symbol, Object> properties) {
        this.properties = properties;
        return this;
    }

    public static Flow decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);

        long incomingWindow = TypeDecoder.getLongField(fields, 1, 0);
        long nextOutgoingId = TypeDecoder.getLongField(fields, 2, 0);
        long outgoingWindow = TypeDecoder.getLongField(fields, 3, 0);

        Flow flow = new Flow(incomingWindow, nextOutgoingId, outgoingWindow);

        Object nextIncomingId = TypeDecoder.getField(fields, 0);
        if (nextIncomingId instanceof Number) {
            flow.nextIncomingId = ((Number) nextIncomingId).longValue();
        }

        Object handle = TypeDecoder.getField(fields, 4);
        if (handle instanceof Number) {
            flow.handle = ((Number) handle).longValue();
        }

        Object deliveryCount = TypeDecoder.getField(fields, 5);
        if (deliveryCount instanceof Number) {
            flow.deliveryCount = ((Number) deliveryCount).longValue();
        }

        Object linkCredit = TypeDecoder.getField(fields, 6);
        if (linkCredit instanceof Number) {
            flow.linkCredit = ((Number) linkCredit).longValue();
        }

        Object available = TypeDecoder.getField(fields, 7);
        if (available instanceof Number) {
            flow.available = ((Number) available).longValue();
        }

        flow.drain = TypeDecoder.getField(fields, 8, Boolean.class);
        flow.echo = TypeDecoder.getField(fields, 9, Boolean.class);

        return flow;
    }

    @Override
    public String toString() {
        if (isLinkFlow()) {
            return String.format("Flow{handle=%d, deliveryCount=%d, linkCredit=%d, drain=%s}",
                    handle, deliveryCount, linkCredit, drain);
        } else {
            return String.format("Flow{nextIncomingId=%s, incomingWindow=%d, nextOutgoingId=%d, outgoingWindow=%d}",
                    nextIncomingId, incomingWindow, nextOutgoingId, outgoingWindow);
        }
    }
}
