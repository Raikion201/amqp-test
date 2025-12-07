package com.amqp.policy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Manages policies across virtual hosts.
 * Policies are applied to queues and exchanges based on pattern matching.
 */
public class PolicyManager {
    private static final Logger logger = LoggerFactory.getLogger(PolicyManager.class);

    // vhost -> policy name -> policy
    private final ConcurrentMap<String, ConcurrentMap<String, Policy>> policies;

    public PolicyManager() {
        this.policies = new ConcurrentHashMap<>();
    }

    /**
     * Add or update a policy.
     */
    public void setPolicy(Policy policy) {
        ConcurrentMap<String, Policy> vhostPolicies =
            policies.computeIfAbsent(policy.getVhost(), k -> new ConcurrentHashMap<>());

        vhostPolicies.put(policy.getName(), policy);
        logger.info("Set policy: {}", policy);
    }

    /**
     * Remove a policy.
     */
    public Policy removePolicy(String vhost, String policyName) {
        ConcurrentMap<String, Policy> vhostPolicies = policies.get(vhost);
        if (vhostPolicies == null) {
            return null;
        }

        Policy removed = vhostPolicies.remove(policyName);
        if (removed != null) {
            logger.info("Removed policy: {} from vhost: {}", policyName, vhost);
        }
        return removed;
    }

    /**
     * Get a specific policy.
     */
    public Policy getPolicy(String vhost, String policyName) {
        ConcurrentMap<String, Policy> vhostPolicies = policies.get(vhost);
        return vhostPolicies != null ? vhostPolicies.get(policyName) : null;
    }

    /**
     * Get all policies for a virtual host.
     */
    public List<Policy> getPolicies(String vhost) {
        ConcurrentMap<String, Policy> vhostPolicies = policies.get(vhost);
        return vhostPolicies != null ?
               new ArrayList<>(vhostPolicies.values()) :
               Collections.emptyList();
    }

    /**
     * Get all policies across all virtual hosts.
     */
    public List<Policy> getAllPolicies() {
        List<Policy> allPolicies = new ArrayList<>();
        for (ConcurrentMap<String, Policy> vhostPolicies : policies.values()) {
            allPolicies.addAll(vhostPolicies.values());
        }
        return allPolicies;
    }

    /**
     * Find the highest priority policy that matches a resource.
     *
     * @param vhost Virtual host name
     * @param resourceName Name of the queue or exchange
     * @param isQueue True if resource is a queue, false if exchange
     * @return The matching policy with highest priority, or null if none match
     */
    public Policy findMatchingPolicy(String vhost, String resourceName, boolean isQueue) {
        ConcurrentMap<String, Policy> vhostPolicies = policies.get(vhost);
        if (vhostPolicies == null || vhostPolicies.isEmpty()) {
            return null;
        }

        return vhostPolicies.values().stream()
            .filter(p -> p.matches(resourceName, isQueue))
            .max(Comparator.comparingInt(Policy::getPriority))
            .orElse(null);
    }

    /**
     * Find all policies that match a resource, sorted by priority.
     */
    public List<Policy> findAllMatchingPolicies(String vhost, String resourceName, boolean isQueue) {
        ConcurrentMap<String, Policy> vhostPolicies = policies.get(vhost);
        if (vhostPolicies == null || vhostPolicies.isEmpty()) {
            return Collections.emptyList();
        }

        return vhostPolicies.values().stream()
            .filter(p -> p.matches(resourceName, isQueue))
            .sorted(Comparator.comparingInt(Policy::getPriority).reversed())
            .collect(Collectors.toList());
    }

    /**
     * Apply policies to queue arguments.
     * Merges policy definitions with explicit queue arguments.
     * Explicit arguments take precedence over policy settings.
     */
    public Map<String, Object> applyQueuePolicies(String vhost, String queueName,
                                                   Map<String, Object> explicitArgs) {
        Map<String, Object> mergedArgs = new HashMap<>();

        // Start with policy-defined arguments
        Policy policy = findMatchingPolicy(vhost, queueName, true);
        if (policy != null) {
            mergedArgs.putAll(policy.getDefinition());
            logger.debug("Applied policy '{}' to queue '{}'", policy.getName(), queueName);
        }

        // Override with explicit arguments
        if (explicitArgs != null) {
            mergedArgs.putAll(explicitArgs);
        }

        return mergedArgs;
    }

    /**
     * Apply policies to exchange arguments.
     */
    public Map<String, Object> applyExchangePolicies(String vhost, String exchangeName,
                                                      Map<String, Object> explicitArgs) {
        Map<String, Object> mergedArgs = new HashMap<>();

        // Start with policy-defined arguments
        Policy policy = findMatchingPolicy(vhost, exchangeName, false);
        if (policy != null) {
            mergedArgs.putAll(policy.getDefinition());
            logger.debug("Applied policy '{}' to exchange '{}'", policy.getName(), exchangeName);
        }

        // Override with explicit arguments
        if (explicitArgs != null) {
            mergedArgs.putAll(explicitArgs);
        }

        return mergedArgs;
    }

    /**
     * Clear all policies for a virtual host.
     */
    public void clearVhostPolicies(String vhost) {
        ConcurrentMap<String, Policy> removed = policies.remove(vhost);
        if (removed != null) {
            logger.info("Cleared {} policies from vhost: {}", removed.size(), vhost);
        }
    }

    /**
     * Clear all policies.
     */
    public void clearAll() {
        int count = policies.values().stream().mapToInt(Map::size).sum();
        policies.clear();
        logger.info("Cleared all {} policies", count);
    }
}
