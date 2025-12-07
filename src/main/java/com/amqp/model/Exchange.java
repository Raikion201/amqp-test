package com.amqp.model;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

public class Exchange {
    private final String name;
    private final Type type;
    private final boolean durable;
    private final boolean autoDelete;
    private final boolean internal;
    private final ConcurrentMap<String, Set<String>> bindings; // routing key -> queue names
    private final ConcurrentMap<String, Set<String>> exchangeBindings; // routing key -> exchange names
    private volatile String alternateExchange;

    public Exchange(String name, Type type, boolean durable, boolean autoDelete, boolean internal) {
        this(name, type, durable, autoDelete, internal, null);
    }

    public Exchange(String name, Type type, boolean durable, boolean autoDelete,
                   boolean internal, String alternateExchange) {
        this.name = name;
        this.type = type;
        this.durable = durable;
        this.autoDelete = autoDelete;
        this.internal = internal;
        this.bindings = new ConcurrentHashMap<>();
        this.exchangeBindings = new ConcurrentHashMap<>();
        this.alternateExchange = alternateExchange;
    }
    
    public String getName() {
        return name;
    }
    
    public Type getType() {
        return type;
    }
    
    public boolean isDurable() {
        return durable;
    }
    
    public boolean isAutoDelete() {
        return autoDelete;
    }
    
    public boolean isInternal() {
        return internal;
    }

    public String getAlternateExchange() {
        return alternateExchange;
    }

    public void setAlternateExchange(String alternateExchange) {
        this.alternateExchange = alternateExchange;
    }

    public boolean hasAlternateExchange() {
        return alternateExchange != null && !alternateExchange.isEmpty();
    }

    public void addBinding(String routingKey, String queueName) {
        bindings.computeIfAbsent(routingKey, k -> ConcurrentHashMap.newKeySet()).add(queueName);
    }
    
    public void removeBinding(String routingKey, String queueName) {
        Set<String> queues = bindings.get(routingKey);
        if (queues != null) {
            queues.remove(queueName);
            if (queues.isEmpty()) {
                bindings.remove(routingKey);
            }
        }
    }

    /**
     * Add an exchange-to-exchange binding.
     */
    public void addExchangeBinding(String routingKey, String exchangeName) {
        exchangeBindings.computeIfAbsent(routingKey, k -> ConcurrentHashMap.newKeySet()).add(exchangeName);
    }

    /**
     * Remove an exchange-to-exchange binding.
     */
    public void removeExchangeBinding(String routingKey, String exchangeName) {
        Set<String> exchanges = exchangeBindings.get(routingKey);
        if (exchanges != null) {
            exchanges.remove(exchangeName);
            if (exchanges.isEmpty()) {
                exchangeBindings.remove(routingKey);
            }
        }
    }

    /**
     * Get all bound exchanges for a routing key (for E2E bindings).
     */
    public Set<String> getBoundExchanges(String routingKey) {
        Set<String> result = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : exchangeBindings.entrySet()) {
            String bindingKey = entry.getKey();
            if (matches(routingKey, bindingKey)) {
                result.addAll(entry.getValue());
            }
        }
        return result;
    }

    /**
     * Get all exchange bindings.
     */
    public Map<String, Set<String>> getAllExchangeBindings() {
        return new HashMap<>(exchangeBindings);
    }

    /**
     * Check if routing key matches binding key based on exchange type.
     */
    private boolean matches(String routingKey, String bindingKey) {
        switch (type) {
            case DIRECT:
                return routingKey.equals(bindingKey);
            case FANOUT:
                return true; // Fanout matches everything
            case TOPIC:
                return matchesTopic(routingKey, bindingKey);
            case HEADERS:
                return false; // Headers exchange needs special handling
            default:
                return false;
        }
    }
    
    public List<String> route(String routingKey) {
        Set<String> result = new HashSet<>();
        
        switch (type) {
            case DIRECT:
                return routeDirect(routingKey);
            case FANOUT:
                return routeFanout();
            case TOPIC:
                return routeTopic(routingKey);
            case HEADERS:
                return routeHeaders(routingKey);
            default:
                return new ArrayList<>();
        }
    }
    
    private List<String> routeDirect(String routingKey) {
        Set<String> queues = bindings.get(routingKey);
        return queues != null ? new ArrayList<>(queues) : new ArrayList<>();
    }
    
    private List<String> routeFanout() {
        Set<String> allQueues = new HashSet<>();
        for (Set<String> queues : bindings.values()) {
            allQueues.addAll(queues);
        }
        return new ArrayList<>(allQueues);
    }
    
    private List<String> routeTopic(String routingKey) {
        Set<String> result = new HashSet<>();
        
        for (Map.Entry<String, Set<String>> entry : bindings.entrySet()) {
            String bindingKey = entry.getKey();
            if (matchesTopic(routingKey, bindingKey)) {
                result.addAll(entry.getValue());
            }
        }
        
        return new ArrayList<>(result);
    }
    
    private boolean matchesTopic(String routingKey, String bindingKey) {
        if (bindingKey.equals("#")) {
            return true;
        }
        
        String[] routingParts = routingKey.split("\\.");
        String[] bindingParts = bindingKey.split("\\.");
        
        return matchesTopicParts(routingParts, bindingParts, 0, 0);
    }
    
    private boolean matchesTopicParts(String[] routingParts, String[] bindingParts, 
                                    int routingIndex, int bindingIndex) {
        if (bindingIndex >= bindingParts.length) {
            return routingIndex >= routingParts.length;
        }
        
        if (routingIndex >= routingParts.length) {
            return bindingParts[bindingIndex].equals("#");
        }
        
        String bindingPart = bindingParts[bindingIndex];
        String routingPart = routingParts[routingIndex];
        
        if (bindingPart.equals("#")) {
            if (bindingIndex == bindingParts.length - 1) {
                return true;
            }
            
            for (int i = routingIndex; i <= routingParts.length; i++) {
                if (matchesTopicParts(routingParts, bindingParts, i, bindingIndex + 1)) {
                    return true;
                }
            }
            return false;
        } else if (bindingPart.equals("*") || bindingPart.equals(routingPart)) {
            return matchesTopicParts(routingParts, bindingParts, routingIndex + 1, bindingIndex + 1);
        } else {
            return false;
        }
    }
    
    private List<String> routeHeaders(String routingKey) {
        return new ArrayList<>();
    }
    
    public enum Type {
        DIRECT,
        FANOUT,
        TOPIC,
        HEADERS
    }
    
    @Override
    public String toString() {
        return String.format("Exchange{name='%s', type=%s, durable=%s, autoDelete=%s, internal=%s, AE='%s'}",
                name, type, durable, autoDelete, internal, alternateExchange);
    }
}