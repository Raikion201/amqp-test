package com.amqp.protocol.v10.messaging;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;

import java.util.HashMap;
import java.util.Map;

/**
 * AMQP 1.0 Message Annotations section.
 *
 * Contains annotations intended for infrastructure use.
 * Keys must be symbols (for interoperability, keys should be prefixed
 * with "x-" for non-standard annotations).
 */
public class MessageAnnotations implements MessageSection {

    public static final long DESCRIPTOR = AmqpType.Descriptor.MESSAGE_ANNOTATIONS;

    private Map<Symbol, Object> annotations;

    public MessageAnnotations() {
        this.annotations = new HashMap<>();
    }

    public MessageAnnotations(Map<Symbol, Object> annotations) {
        this.annotations = annotations != null ? new HashMap<>(annotations) : new HashMap<>();
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public SectionType getSectionType() {
        return SectionType.MESSAGE_ANNOTATIONS;
    }

    @Override
    public DescribedType toDescribed() {
        return new DescribedType.Default(DESCRIPTOR, annotations);
    }

    // Map operations
    public Map<Symbol, Object> getValue() {
        return annotations;
    }

    public Object get(Symbol key) {
        return annotations.get(key);
    }

    public Object get(String key) {
        return annotations.get(Symbol.valueOf(key));
    }

    public MessageAnnotations put(Symbol key, Object value) {
        annotations.put(key, value);
        return this;
    }

    public MessageAnnotations put(String key, Object value) {
        annotations.put(Symbol.valueOf(key), value);
        return this;
    }

    public Object remove(Symbol key) {
        return annotations.remove(key);
    }

    public boolean containsKey(Symbol key) {
        return annotations.containsKey(key);
    }

    public int size() {
        return annotations.size();
    }

    public boolean isEmpty() {
        return annotations.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public static MessageAnnotations decode(DescribedType described) {
        Object value = described.getDescribed();
        if (value instanceof Map) {
            Map<Symbol, Object> annotations = new HashMap<>();
            Map<?, ?> rawMap = (Map<?, ?>) value;
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                Symbol key;
                if (entry.getKey() instanceof Symbol) {
                    key = (Symbol) entry.getKey();
                } else if (entry.getKey() instanceof String) {
                    key = Symbol.valueOf((String) entry.getKey());
                } else {
                    continue;
                }
                annotations.put(key, entry.getValue());
            }
            return new MessageAnnotations(annotations);
        }
        return new MessageAnnotations();
    }

    @Override
    public String toString() {
        return "MessageAnnotations{" + annotations + "}";
    }
}
