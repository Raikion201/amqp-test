package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;
import com.amqp.protocol.v10.types.TypeDecoder;
import com.amqp.protocol.v10.types.UInt;

import java.util.*;

/**
 * AMQP 1.0 Target terminus.
 *
 * Describes the target of a link.
 *
 * Fields:
 * 0: address (string) - Address of the target
 * 1: durable (terminus-durability) - Durability of the target
 * 2: expiry-policy (terminus-expiry-policy) - Expiry policy
 * 3: timeout (seconds) - Timeout in seconds
 * 4: dynamic (boolean) - Request dynamic creation
 * 5: dynamic-node-properties (node-properties) - Properties for dynamic node
 * 6: capabilities (symbol[]) - Capabilities
 */
public class Target {

    public static final long DESCRIPTOR = AmqpType.Descriptor.TARGET;

    // Terminus durability (same as Source)
    public static final int DURABILITY_NONE = 0;
    public static final int DURABILITY_CONFIGURATION = 1;
    public static final int DURABILITY_UNSETTLED_STATE = 2;

    // Expiry policies (same as Source)
    public static final Symbol EXPIRY_LINK_DETACH = Symbol.valueOf("link-detach");
    public static final Symbol EXPIRY_SESSION_END = Symbol.valueOf("session-end");
    public static final Symbol EXPIRY_CONNECTION_CLOSE = Symbol.valueOf("connection-close");
    public static final Symbol EXPIRY_NEVER = Symbol.valueOf("never");

    private String address;
    private Integer durable;
    private Symbol expiryPolicy;
    private Long timeout;
    private Boolean dynamic;
    private Map<Symbol, Object> dynamicNodeProperties;
    private Symbol[] capabilities;

    public Target() {
    }

    public Target(String address) {
        this.address = address;
    }

    // Getters
    public String getAddress() {
        return address;
    }

    public Integer getDurable() {
        return durable;
    }

    public Symbol getExpiryPolicy() {
        return expiryPolicy;
    }

    public Long getTimeout() {
        return timeout;
    }

    public Boolean getDynamic() {
        return dynamic;
    }

    public Map<Symbol, Object> getDynamicNodeProperties() {
        return dynamicNodeProperties;
    }

    public Symbol[] getCapabilities() {
        return capabilities;
    }

    // Setters
    public Target setAddress(String address) {
        this.address = address;
        return this;
    }

    public Target setDurable(Integer durable) {
        this.durable = durable;
        return this;
    }

    public Target setExpiryPolicy(Symbol expiryPolicy) {
        this.expiryPolicy = expiryPolicy;
        return this;
    }

    public Target setTimeout(Long timeout) {
        this.timeout = timeout;
        return this;
    }

    public Target setDynamic(Boolean dynamic) {
        this.dynamic = dynamic;
        return this;
    }

    public Target setDynamicNodeProperties(Map<Symbol, Object> properties) {
        this.dynamicNodeProperties = properties;
        return this;
    }

    public Target setCapabilities(Symbol... capabilities) {
        this.capabilities = capabilities;
        return this;
    }

    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(address);
        // durable is terminus-durability (uint) per AMQP 1.0 spec
        fields.add(durable == null ? null : UInt.valueOf(durable));
        fields.add(expiryPolicy);
        // timeout is seconds (uint) per AMQP 1.0 spec
        fields.add(timeout == null ? null : UInt.valueOf(timeout));
        fields.add(dynamic);
        fields.add(dynamicNodeProperties);
        fields.add(capabilities);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    @SuppressWarnings("unchecked")
    public static Target decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        Target target = new Target();

        target.address = TypeDecoder.getField(fields, 0, String.class);

        Object durableField = TypeDecoder.getField(fields, 1);
        if (durableField instanceof Number) {
            target.durable = ((Number) durableField).intValue();
        }

        Object expiryField = TypeDecoder.getField(fields, 2);
        if (expiryField instanceof Symbol) {
            target.expiryPolicy = (Symbol) expiryField;
        } else if (expiryField instanceof String) {
            target.expiryPolicy = Symbol.valueOf((String) expiryField);
        }

        Object timeoutField = TypeDecoder.getField(fields, 3);
        if (timeoutField instanceof Number) {
            target.timeout = ((Number) timeoutField).longValue();
        }

        target.dynamic = TypeDecoder.getField(fields, 4, Boolean.class);

        Object dynPropsField = TypeDecoder.getField(fields, 5);
        if (dynPropsField instanceof Map) {
            target.dynamicNodeProperties = (Map<Symbol, Object>) dynPropsField;
        }

        // capabilities array would need array handling

        return target;
    }

    @Override
    public String toString() {
        return String.format("Target{address='%s', durable=%s, dynamic=%s}",
                address, durable, dynamic);
    }
}
