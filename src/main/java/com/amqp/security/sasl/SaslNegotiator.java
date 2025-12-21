package com.amqp.security.sasl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * SASL mechanism negotiator.
 *
 * Manages available mechanisms and handles mechanism selection.
 */
public class SaslNegotiator {

    private static final Logger log = LoggerFactory.getLogger(SaslNegotiator.class);

    private final Map<String, SaslMechanism> mechanisms = new LinkedHashMap<>();
    private final List<String> mechanismOrder = new ArrayList<>();

    public SaslNegotiator() {
    }

    /**
     * Register a SASL mechanism.
     */
    public void addMechanism(SaslMechanism mechanism) {
        mechanisms.put(mechanism.getName(), mechanism);
        updateOrder();
        log.debug("Registered SASL mechanism: {}", mechanism.getName());
    }

    /**
     * Remove a SASL mechanism.
     */
    public void removeMechanism(String name) {
        mechanisms.remove(name);
        updateOrder();
    }

    private void updateOrder() {
        // Sort by priority (descending)
        mechanismOrder.clear();
        mechanismOrder.addAll(
                mechanisms.values().stream()
                        .sorted((a, b) -> Integer.compare(b.getPriority(), a.getPriority()))
                        .map(SaslMechanism::getName)
                        .collect(Collectors.toList())
        );
    }

    /**
     * Get the list of applicable mechanisms for a context.
     */
    public List<String> getApplicableMechanisms(SaslContext context) {
        return mechanismOrder.stream()
                .filter(name -> mechanisms.get(name).isApplicable(context))
                .collect(Collectors.toList());
    }

    /**
     * Get the mechanism names in priority order.
     */
    public List<String> getMechanismNames() {
        return new ArrayList<>(mechanismOrder);
    }

    /**
     * Get a mechanism by name.
     */
    public SaslMechanism getMechanism(String name) {
        return mechanisms.get(name);
    }

    /**
     * Check if a mechanism is available.
     */
    public boolean hasMechanism(String name) {
        return mechanisms.containsKey(name);
    }

    /**
     * Handle mechanism selection and initial response.
     */
    public SaslMechanism.SaslResult handleInit(SaslContext context, String mechanismName,
                                                byte[] initialResponse) {
        SaslMechanism mechanism = mechanisms.get(mechanismName);
        if (mechanism == null) {
            log.warn("Unknown SASL mechanism: {}", mechanismName);
            return SaslMechanism.SaslResult.authFailed("Unknown mechanism: " + mechanismName);
        }

        if (!mechanism.isApplicable(context)) {
            log.warn("SASL mechanism {} not applicable in this context", mechanismName);
            return SaslMechanism.SaslResult.authFailed("Mechanism not applicable");
        }

        context.setSelectedMechanism(mechanismName);
        log.debug("Selected SASL mechanism: {}", mechanismName);

        return mechanism.handleResponse(context, initialResponse);
    }

    /**
     * Handle a response to a challenge.
     */
    public SaslMechanism.SaslResult handleResponse(SaslContext context, byte[] response) {
        String mechanismName = context.getSelectedMechanism();
        if (mechanismName == null) {
            return SaslMechanism.SaslResult.authFailed("No mechanism selected");
        }

        SaslMechanism mechanism = mechanisms.get(mechanismName);
        if (mechanism == null) {
            return SaslMechanism.SaslResult.authFailed("Mechanism not found");
        }

        return mechanism.handleChallengeResponse(context, response);
    }

    /**
     * Create a default negotiator with common mechanisms.
     */
    public static SaslNegotiator createDefault(
            java.util.function.BiFunction<String, String, Boolean> plainAuth,
            java.util.function.Function<String, ScramMechanism.ScramCredentials> scramLookup) {

        SaslNegotiator negotiator = new SaslNegotiator();

        // Add mechanisms in preference order
        if (scramLookup != null) {
            negotiator.addMechanism(new ScramMechanism(scramLookup));
        }

        negotiator.addMechanism(new ExternalMechanism());

        if (plainAuth != null) {
            negotiator.addMechanism(new PlainMechanism(plainAuth));
        }

        return negotiator;
    }
}
