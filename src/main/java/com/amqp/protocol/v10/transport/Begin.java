package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.*;

/**
 * AMQP 1.0 Begin performative.
 *
 * Sent to begin a session on a connection.
 *
 * Fields:
 * 0: remote-channel (ushort) - Remote endpoint's channel for this session
 * 1: next-outgoing-id (transfer-number, mandatory) - Next outgoing transfer ID
 * 2: incoming-window (uint, mandatory) - Incoming window size
 * 3: outgoing-window (uint, mandatory) - Outgoing window size
 * 4: handle-max (handle) - Maximum handle number
 * 5: offered-capabilities (symbol[]) - Offered capabilities
 * 6: desired-capabilities (symbol[]) - Desired capabilities
 * 7: properties (map) - Session properties
 */
public class Begin implements Performative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.BEGIN;

    public static final long DEFAULT_HANDLE_MAX = 0xFFFFFFFFL; // uint max

    private Integer remoteChannel;
    private long nextOutgoingId;
    private long incomingWindow;
    private long outgoingWindow;
    private long handleMax = DEFAULT_HANDLE_MAX;
    private Symbol[] offeredCapabilities;
    private Symbol[] desiredCapabilities;
    private Map<Symbol, Object> properties;

    public Begin(long nextOutgoingId, long incomingWindow, long outgoingWindow) {
        this.nextOutgoingId = nextOutgoingId;
        this.incomingWindow = incomingWindow;
        this.outgoingWindow = outgoingWindow;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public List<Object> getFields() {
        List<Object> fields = new ArrayList<>();
        fields.add(remoteChannel);
        fields.add(nextOutgoingId);
        fields.add(incomingWindow);
        fields.add(outgoingWindow);
        fields.add(handleMax == DEFAULT_HANDLE_MAX ? null : handleMax);
        fields.add(offeredCapabilities);
        fields.add(desiredCapabilities);
        fields.add(properties);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return fields;
    }

    // Getters
    public Integer getRemoteChannel() {
        return remoteChannel;
    }

    public long getNextOutgoingId() {
        return nextOutgoingId;
    }

    public long getIncomingWindow() {
        return incomingWindow;
    }

    public long getOutgoingWindow() {
        return outgoingWindow;
    }

    public long getHandleMax() {
        return handleMax;
    }

    public Symbol[] getOfferedCapabilities() {
        return offeredCapabilities;
    }

    public Symbol[] getDesiredCapabilities() {
        return desiredCapabilities;
    }

    public Map<Symbol, Object> getProperties() {
        return properties;
    }

    // Setters
    public Begin setRemoteChannel(Integer remoteChannel) {
        this.remoteChannel = remoteChannel;
        return this;
    }

    public Begin setHandleMax(long handleMax) {
        this.handleMax = handleMax;
        return this;
    }

    public Begin setOfferedCapabilities(Symbol... capabilities) {
        this.offeredCapabilities = capabilities;
        return this;
    }

    public Begin setDesiredCapabilities(Symbol... capabilities) {
        this.desiredCapabilities = capabilities;
        return this;
    }

    public Begin setProperties(Map<Symbol, Object> properties) {
        this.properties = properties;
        return this;
    }

    public static Begin decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);

        long nextOutgoingId = TypeDecoder.getLongField(fields, 1, 0);
        long incomingWindow = TypeDecoder.getLongField(fields, 2, 0);
        long outgoingWindow = TypeDecoder.getLongField(fields, 3, 0);

        Begin begin = new Begin(nextOutgoingId, incomingWindow, outgoingWindow);

        Object remoteChannel = TypeDecoder.getField(fields, 0);
        if (remoteChannel instanceof Number) {
            begin.remoteChannel = ((Number) remoteChannel).intValue();
        }

        begin.handleMax = TypeDecoder.getLongField(fields, 4, DEFAULT_HANDLE_MAX);

        return begin;
    }

    @Override
    public String toString() {
        return String.format("Begin{remoteChannel=%s, nextOutgoingId=%d, incomingWindow=%d, outgoingWindow=%d, handleMax=%d}",
                remoteChannel, nextOutgoingId, incomingWindow, outgoingWindow, handleMax);
    }
}
