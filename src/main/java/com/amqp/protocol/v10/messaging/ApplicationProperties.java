package com.amqp.protocol.v10.messaging;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AMQP 1.0 Application Properties section.
 *
 * Contains application-specific key-value properties.
 * Keys must be strings, values can be any simple AMQP type.
 */
public class ApplicationProperties implements MessageSection {

    public static final long DESCRIPTOR = AmqpType.Descriptor.APPLICATION_PROPERTIES;

    private Map<String, Object> properties;

    public ApplicationProperties() {
        this.properties = new HashMap<>();
    }

    public ApplicationProperties(Map<String, Object> properties) {
        this.properties = properties != null ? new HashMap<>(properties) : new HashMap<>();
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public SectionType getSectionType() {
        return SectionType.APPLICATION_PROPERTIES;
    }

    @Override
    public DescribedType toDescribed() {
        return new DescribedType.Default(DESCRIPTOR, properties);
    }

    // Map operations
    public Map<String, Object> getValue() {
        return properties;
    }

    public Object get(String key) {
        return properties.get(key);
    }

    public String getString(String key) {
        Object value = properties.get(key);
        return value instanceof String ? (String) value : null;
    }

    public Long getLong(String key) {
        Object value = properties.get(key);
        return value instanceof Number ? ((Number) value).longValue() : null;
    }

    public Integer getInt(String key) {
        Object value = properties.get(key);
        return value instanceof Number ? ((Number) value).intValue() : null;
    }

    public Boolean getBoolean(String key) {
        Object value = properties.get(key);
        return value instanceof Boolean ? (Boolean) value : null;
    }

    public ApplicationProperties put(String key, Object value) {
        properties.put(key, value);
        return this;
    }

    public Object remove(String key) {
        return properties.remove(key);
    }

    public boolean containsKey(String key) {
        return properties.containsKey(key);
    }

    public int size() {
        return properties.size();
    }

    public boolean isEmpty() {
        return properties.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public static ApplicationProperties decode(DescribedType described) {
        Object value = described.getDescribed();
        if (value instanceof Map) {
            Map<String, Object> props = new HashMap<>();
            Map<?, ?> rawMap = (Map<?, ?>) value;
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                if (entry.getKey() instanceof String) {
                    props.put((String) entry.getKey(), entry.getValue());
                } else if (entry.getKey() != null) {
                    props.put(entry.getKey().toString(), entry.getValue());
                }
            }
            return new ApplicationProperties(props);
        } else if (value instanceof List) {
            // Some encoders send list of pairs
            return new ApplicationProperties();
        }
        return new ApplicationProperties();
    }

    @Override
    public String toString() {
        return "ApplicationProperties{" + properties + "}";
    }
}
