package com.amqp.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a binding from one exchange to another exchange (E2E binding).
 * This is a RabbitMQ extension to AMQP 0.9.1.
 */
public class ExchangeBinding {
    private final String sourceExchange;
    private final String destinationExchange;
    private final String routingKey;
    private final Map<String, Object> arguments;

    public ExchangeBinding(String sourceExchange, String destinationExchange,
                          String routingKey, Map<String, Object> arguments) {
        this.sourceExchange = sourceExchange;
        this.destinationExchange = destinationExchange;
        this.routingKey = routingKey != null ? routingKey : "";
        this.arguments = arguments != null ? new HashMap<>(arguments) : new HashMap<>();
    }

    public String getSourceExchange() {
        return sourceExchange;
    }

    public String getDestinationExchange() {
        return destinationExchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public Map<String, Object> getArguments() {
        return new HashMap<>(arguments);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExchangeBinding that = (ExchangeBinding) o;
        return Objects.equals(sourceExchange, that.sourceExchange) &&
               Objects.equals(destinationExchange, that.destinationExchange) &&
               Objects.equals(routingKey, that.routingKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceExchange, destinationExchange, routingKey);
    }

    @Override
    public String toString() {
        return String.format("ExchangeBinding{source='%s', destination='%s', routingKey='%s'}",
                           sourceExchange, destinationExchange, routingKey);
    }
}
