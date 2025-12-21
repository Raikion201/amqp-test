package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.*;

/**
 * AMQP 1.0 Open performative.
 *
 * Sent to open a connection between two peers.
 *
 * Fields:
 * 0: container-id (string, mandatory) - Unique identifier for the container
 * 1: hostname (string) - Virtual host name
 * 2: max-frame-size (uint) - Maximum frame size in bytes
 * 3: channel-max (ushort) - Maximum channel number
 * 4: idle-time-out (milliseconds) - Idle timeout in milliseconds
 * 5: outgoing-locales (symbol[]) - Supported locales for outgoing text
 * 6: incoming-locales (symbol[]) - Desired locales for incoming text
 * 7: offered-capabilities (symbol[]) - Capabilities offered by sender
 * 8: desired-capabilities (symbol[]) - Capabilities desired by sender
 * 9: properties (map) - Connection properties
 */
public class Open implements Performative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.OPEN;

    // Default values per AMQP 1.0 spec
    public static final long DEFAULT_MAX_FRAME_SIZE = 0xFFFFFFFFL; // uint max (4GB)
    public static final int DEFAULT_CHANNEL_MAX = 65535;
    public static final long DEFAULT_IDLE_TIMEOUT = 0; // No timeout

    private final String containerId;
    private String hostname;
    private long maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
    private int channelMax = DEFAULT_CHANNEL_MAX;
    private long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    private Symbol[] outgoingLocales;
    private Symbol[] incomingLocales;
    private Symbol[] offeredCapabilities;
    private Symbol[] desiredCapabilities;
    private Map<Symbol, Object> properties;

    public Open(String containerId) {
        this.containerId = Objects.requireNonNull(containerId, "containerId is required");
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public List<Object> getFields() {
        List<Object> fields = new ArrayList<>();
        fields.add(containerId);
        fields.add(hostname);
        fields.add(maxFrameSize == DEFAULT_MAX_FRAME_SIZE ? null : maxFrameSize);
        fields.add(channelMax == DEFAULT_CHANNEL_MAX ? null : channelMax);
        fields.add(idleTimeout == DEFAULT_IDLE_TIMEOUT ? null : idleTimeout);
        fields.add(outgoingLocales);
        fields.add(incomingLocales);
        fields.add(offeredCapabilities);
        fields.add(desiredCapabilities);
        fields.add(properties);

        // Trim trailing nulls
        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return fields;
    }

    // Getters
    public String getContainerId() {
        return containerId;
    }

    public String getHostname() {
        return hostname;
    }

    public long getMaxFrameSize() {
        return maxFrameSize;
    }

    public int getChannelMax() {
        return channelMax;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public Symbol[] getOutgoingLocales() {
        return outgoingLocales;
    }

    public Symbol[] getIncomingLocales() {
        return incomingLocales;
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

    // Setters (builder pattern)
    public Open setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public Open setMaxFrameSize(long maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
        return this;
    }

    public Open setChannelMax(int channelMax) {
        this.channelMax = channelMax;
        return this;
    }

    public Open setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    }

    public Open setOutgoingLocales(Symbol... locales) {
        this.outgoingLocales = locales;
        return this;
    }

    public Open setIncomingLocales(Symbol... locales) {
        this.incomingLocales = locales;
        return this;
    }

    public Open setOfferedCapabilities(Symbol... capabilities) {
        this.offeredCapabilities = capabilities;
        return this;
    }

    public Open setDesiredCapabilities(Symbol... capabilities) {
        this.desiredCapabilities = capabilities;
        return this;
    }

    public Open setProperties(Map<Symbol, Object> properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Decode an Open performative from a described type.
     */
    @SuppressWarnings("unchecked")
    public static Open decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);

        String containerId = TypeDecoder.getField(fields, 0, String.class);
        if (containerId == null) {
            throw new IllegalArgumentException("container-id is required");
        }

        Open open = new Open(containerId);
        open.hostname = TypeDecoder.getField(fields, 1, String.class);
        open.maxFrameSize = TypeDecoder.getLongField(fields, 2, DEFAULT_MAX_FRAME_SIZE);
        open.channelMax = TypeDecoder.getIntField(fields, 3, DEFAULT_CHANNEL_MAX);
        open.idleTimeout = TypeDecoder.getLongField(fields, 4, DEFAULT_IDLE_TIMEOUT);

        // Arrays and maps (simplified)
        Object outLocales = TypeDecoder.getField(fields, 5);
        if (outLocales instanceof Object[]) {
            open.outgoingLocales = toSymbolArray((Object[]) outLocales);
        }

        Object inLocales = TypeDecoder.getField(fields, 6);
        if (inLocales instanceof Object[]) {
            open.incomingLocales = toSymbolArray((Object[]) inLocales);
        }

        Object offered = TypeDecoder.getField(fields, 7);
        if (offered instanceof Object[]) {
            open.offeredCapabilities = toSymbolArray((Object[]) offered);
        }

        Object desired = TypeDecoder.getField(fields, 8);
        if (desired instanceof Object[]) {
            open.desiredCapabilities = toSymbolArray((Object[]) desired);
        }

        Object props = TypeDecoder.getField(fields, 9);
        if (props instanceof Map) {
            open.properties = (Map<Symbol, Object>) props;
        }

        return open;
    }

    private static Symbol[] toSymbolArray(Object[] objects) {
        Symbol[] symbols = new Symbol[objects.length];
        for (int i = 0; i < objects.length; i++) {
            if (objects[i] instanceof Symbol) {
                symbols[i] = (Symbol) objects[i];
            } else if (objects[i] instanceof String) {
                symbols[i] = Symbol.valueOf((String) objects[i]);
            }
        }
        return symbols;
    }

    @Override
    public String toString() {
        return String.format("Open{containerId='%s', hostname='%s', maxFrameSize=%d, channelMax=%d, idleTimeout=%d}",
                containerId, hostname, maxFrameSize, channelMax, idleTimeout);
    }
}
