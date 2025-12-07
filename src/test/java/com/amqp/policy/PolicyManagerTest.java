package com.amqp.policy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PolicyManager.
 */
class PolicyManagerTest {

    private PolicyManager policyManager;

    @BeforeEach
    void setUp() {
        policyManager = new PolicyManager();
    }

    @Test
    void testSetAndGetPolicy() {
        Map<String, Object> definition = new HashMap<>();
        definition.put("max-length", 1000);
        definition.put("message-ttl", 60000);

        Policy policy = new Policy("test-policy", "/", "^test.*", Policy.ApplyTo.QUEUES, definition, 1);
        policyManager.setPolicy(policy);

        Policy retrieved = policyManager.getPolicy("/", "test-policy");
        assertNotNull(retrieved);
        assertEquals("test-policy", retrieved.getName());
        assertEquals(1, retrieved.getPriority());
    }

    @Test
    void testRemovePolicy() {
        Map<String, Object> definition = new HashMap<>();
        definition.put("max-length", 1000);

        Policy policy = new Policy("test-policy", "/", ".*", Policy.ApplyTo.QUEUES, definition, 1);
        policyManager.setPolicy(policy);

        Policy removed = policyManager.removePolicy("/", "test-policy");
        assertNotNull(removed);
        assertEquals("test-policy", removed.getName());

        Policy retrieved = policyManager.getPolicy("/", "test-policy");
        assertNull(retrieved);
    }

    @Test
    void testFindMatchingPolicy() {
        Map<String, Object> definition = new HashMap<>();
        definition.put("max-length", 1000);

        Policy policy = new Policy("test-policy", "/", "^test.*", Policy.ApplyTo.QUEUES, definition, 1);
        policyManager.setPolicy(policy);

        Policy matched = policyManager.findMatchingPolicy("/", "test-queue", true);
        assertNotNull(matched);
        assertEquals("test-policy", matched.getName());

        Policy notMatched = policyManager.findMatchingPolicy("/", "other-queue", true);
        assertNull(notMatched);
    }

    @Test
    void testPolicyPriority() {
        Map<String, Object> definition1 = new HashMap<>();
        definition1.put("max-length", 1000);

        Map<String, Object> definition2 = new HashMap<>();
        definition2.put("max-length", 2000);

        Policy policy1 = new Policy("low-priority", "/", ".*", Policy.ApplyTo.QUEUES, definition1, 1);
        Policy policy2 = new Policy("high-priority", "/", ".*", Policy.ApplyTo.QUEUES, definition2, 10);

        policyManager.setPolicy(policy1);
        policyManager.setPolicy(policy2);

        Policy matched = policyManager.findMatchingPolicy("/", "any-queue", true);
        assertNotNull(matched);
        assertEquals("high-priority", matched.getName());
        assertEquals(10, matched.getPriority());
    }

    @Test
    void testApplyQueuePolicies() {
        Map<String, Object> policyDef = new HashMap<>();
        policyDef.put("max-length", 1000);
        policyDef.put("message-ttl", 60000);

        Policy policy = new Policy("test-policy", "/", ".*", Policy.ApplyTo.QUEUES, policyDef, 1);
        policyManager.setPolicy(policy);

        Map<String, Object> explicitArgs = new HashMap<>();
        explicitArgs.put("max-length", 2000); // Override policy

        Map<String, Object> merged = policyManager.applyQueuePolicies("/", "test-queue", explicitArgs);

        assertEquals(2000, merged.get("max-length")); // Explicit overrides policy
        assertEquals(60000, merged.get("message-ttl")); // From policy
    }

    @Test
    void testApplyToFilter() {
        Map<String, Object> definition = new HashMap<>();
        definition.put("max-length", 1000);

        Policy queuePolicy = new Policy("queue-policy", "/", ".*", Policy.ApplyTo.QUEUES, definition, 1);
        policyManager.setPolicy(queuePolicy);

        // Should match queues
        Policy matched = policyManager.findMatchingPolicy("/", "test-queue", true);
        assertNotNull(matched);

        // Should not match exchanges
        Policy notMatched = policyManager.findMatchingPolicy("/", "test-exchange", false);
        assertNull(notMatched);
    }

    @Test
    void testGetAllPolicies() {
        Policy policy1 = new Policy("policy1", "/", ".*", Policy.ApplyTo.QUEUES, new HashMap<>(), 1);
        Policy policy2 = new Policy("policy2", "/", ".*", Policy.ApplyTo.EXCHANGES, new HashMap<>(), 1);
        Policy policy3 = new Policy("policy3", "/other", ".*", Policy.ApplyTo.QUEUES, new HashMap<>(), 1);

        policyManager.setPolicy(policy1);
        policyManager.setPolicy(policy2);
        policyManager.setPolicy(policy3);

        List<Policy> allPolicies = policyManager.getAllPolicies();
        assertEquals(3, allPolicies.size());

        List<Policy> vhostPolicies = policyManager.getPolicies("/");
        assertEquals(2, vhostPolicies.size());
    }

    @Test
    void testClearVhostPolicies() {
        Policy policy1 = new Policy("policy1", "/", ".*", Policy.ApplyTo.QUEUES, new HashMap<>(), 1);
        Policy policy2 = new Policy("policy2", "/other", ".*", Policy.ApplyTo.QUEUES, new HashMap<>(), 1);

        policyManager.setPolicy(policy1);
        policyManager.setPolicy(policy2);

        policyManager.clearVhostPolicies("/");

        List<Policy> vhostPolicies = policyManager.getPolicies("/");
        assertTrue(vhostPolicies.isEmpty());

        List<Policy> otherPolicies = policyManager.getPolicies("/other");
        assertEquals(1, otherPolicies.size());
    }
}
