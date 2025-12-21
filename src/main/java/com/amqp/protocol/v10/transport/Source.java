package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.*;

/**
 * AMQP 1.0 Source terminus.
 *
 * Describes the source of a link.
 *
 * Fields:
 * 0: address (string) - Address of the source
 * 1: durable (terminus-durability) - Durability of the source
 * 2: expiry-policy (terminus-expiry-policy) - Expiry policy
 * 3: timeout (seconds) - Timeout in seconds
 * 4: dynamic (boolean) - Request dynamic creation
 * 5: dynamic-node-properties (node-properties) - Properties for dynamic node
 * 6: distribution-mode (symbol) - Distribution mode
 * 7: filter (filter-set) - Filter set
 * 8: default-outcome (outcome) - Default outcome
 * 9: outcomes (symbol[]) - Supported outcomes
 * 10: capabilities (symbol[]) - Capabilities
 */
public class Source {

    public static final long DESCRIPTOR = AmqpType.Descriptor.SOURCE;

    // Terminus durability
    public static final int DURABILITY_NONE = 0;
    public static final int DURABILITY_CONFIGURATION = 1;
    public static final int DURABILITY_UNSETTLED_STATE = 2;

    // Expiry policies
    public static final Symbol EXPIRY_LINK_DETACH = Symbol.valueOf("link-detach");
    public static final Symbol EXPIRY_SESSION_END = Symbol.valueOf("session-end");
    public static final Symbol EXPIRY_CONNECTION_CLOSE = Symbol.valueOf("connection-close");
    public static final Symbol EXPIRY_NEVER = Symbol.valueOf("never");

    // Distribution modes
    public static final Symbol DIST_MODE_MOVE = Symbol.valueOf("move");
    public static final Symbol DIST_MODE_COPY = Symbol.valueOf("copy");

    private String address;
    private Integer durable;
    private Symbol expiryPolicy;
    private Long timeout;
    private Boolean dynamic;
    private Map<Symbol, Object> dynamicNodeProperties;
    private Symbol distributionMode;
    private Map<Symbol, Object> filter;
    private Object defaultOutcome;
    private Symbol[] outcomes;
    private Symbol[] capabilities;

    public Source() {
    }

    public Source(String address) {
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

    public Symbol getDistributionMode() {
        return distributionMode;
    }

    public Map<Symbol, Object> getFilter() {
        return filter;
    }

    public Object getDefaultOutcome() {
        return defaultOutcome;
    }

    public Symbol[] getOutcomes() {
        return outcomes;
    }

    public Symbol[] getCapabilities() {
        return capabilities;
    }

    // Setters
    public Source setAddress(String address) {
        this.address = address;
        return this;
    }

    public Source setDurable(Integer durable) {
        this.durable = durable;
        return this;
    }

    public Source setExpiryPolicy(Symbol expiryPolicy) {
        this.expiryPolicy = expiryPolicy;
        return this;
    }

    public Source setTimeout(Long timeout) {
        this.timeout = timeout;
        return this;
    }

    public Source setDynamic(Boolean dynamic) {
        this.dynamic = dynamic;
        return this;
    }

    public Source setDynamicNodeProperties(Map<Symbol, Object> properties) {
        this.dynamicNodeProperties = properties;
        return this;
    }

    public Source setDistributionMode(Symbol distributionMode) {
        this.distributionMode = distributionMode;
        return this;
    }

    public Source setFilter(Map<Symbol, Object> filter) {
        this.filter = filter;
        return this;
    }

    public Source setDefaultOutcome(Object defaultOutcome) {
        this.defaultOutcome = defaultOutcome;
        return this;
    }

    public Source setOutcomes(Symbol... outcomes) {
        this.outcomes = outcomes;
        return this;
    }

    public Source setCapabilities(Symbol... capabilities) {
        this.capabilities = capabilities;
        return this;
    }

    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(address);
        fields.add(durable);
        fields.add(expiryPolicy);
        fields.add(timeout);
        fields.add(dynamic);
        fields.add(dynamicNodeProperties);
        fields.add(distributionMode);
        fields.add(filter);
        fields.add(defaultOutcome);
        fields.add(outcomes);
        fields.add(capabilities);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    @SuppressWarnings("unchecked")
    public static Source decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        Source source = new Source();

        source.address = TypeDecoder.getField(fields, 0, String.class);

        Object durableField = TypeDecoder.getField(fields, 1);
        if (durableField instanceof Number) {
            source.durable = ((Number) durableField).intValue();
        }

        Object expiryField = TypeDecoder.getField(fields, 2);
        if (expiryField instanceof Symbol) {
            source.expiryPolicy = (Symbol) expiryField;
        } else if (expiryField instanceof String) {
            source.expiryPolicy = Symbol.valueOf((String) expiryField);
        }

        Object timeoutField = TypeDecoder.getField(fields, 3);
        if (timeoutField instanceof Number) {
            source.timeout = ((Number) timeoutField).longValue();
        }

        source.dynamic = TypeDecoder.getField(fields, 4, Boolean.class);

        Object dynPropsField = TypeDecoder.getField(fields, 5);
        if (dynPropsField instanceof Map) {
            source.dynamicNodeProperties = (Map<Symbol, Object>) dynPropsField;
        }

        Object distModeField = TypeDecoder.getField(fields, 6);
        if (distModeField instanceof Symbol) {
            source.distributionMode = (Symbol) distModeField;
        } else if (distModeField instanceof String) {
            source.distributionMode = Symbol.valueOf((String) distModeField);
        }

        Object filterField = TypeDecoder.getField(fields, 7);
        if (filterField instanceof Map) {
            source.filter = (Map<Symbol, Object>) filterField;
        }

        source.defaultOutcome = TypeDecoder.getField(fields, 8);

        // outcomes and capabilities arrays would need array handling

        return source;
    }

    @Override
    public String toString() {
        return String.format("Source{address='%s', durable=%s, dynamic=%s}",
                address, durable, dynamic);
    }
}
