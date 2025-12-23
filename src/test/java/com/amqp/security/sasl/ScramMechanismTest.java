/*
 * Adapted from Apache ActiveMQ Artemis SASL tests.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.security.sasl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SASL SCRAM-SHA-256 authentication mechanism.
 * Adapted from Apache ActiveMQ Artemis SCRAMTest.
 */
@DisplayName("SASL SCRAM-SHA-256 Mechanism Tests")
public class ScramMechanismTest {

    private Map<String, ScramMechanism.ScramCredentials> credentialsStore;
    private ScramMechanism mechanism;
    private SaslContext context;
    private SecureRandom random;

    @BeforeEach
    void setUp() throws Exception {
        credentialsStore = new HashMap<>();
        random = new SecureRandom();

        // Create test credentials
        byte[] salt = new byte[16];
        random.nextBytes(salt);
        ScramMechanism.ScramCredentials guestCreds =
                ScramMechanism.createCredentials("guest", salt, 4096);
        credentialsStore.put("guest", guestCreds);

        salt = new byte[16];
        random.nextBytes(salt);
        ScramMechanism.ScramCredentials adminCreds =
                ScramMechanism.createCredentials("adminpass", salt, 4096);
        credentialsStore.put("admin", adminCreds);

        mechanism = new ScramMechanism(username -> credentialsStore.get(username));
        context = new SimpleSaslContext();
    }

    @Test
    @DisplayName("SCRAM: Name should be 'SCRAM-SHA-256'")
    void testMechanismName() {
        assertEquals("SCRAM-SHA-256", mechanism.getName());
    }

    @Test
    @DisplayName("SCRAM: Should return challenge on valid client-first-message")
    void testClientFirstMessageReturnsChallenge() {
        // Client-first-message: n,,n=guest,r=clientnonce
        String clientNonce = generateNonce();
        String clientFirstMessage = "n,,n=guest,r=" + clientNonce;

        SaslResult result = mechanism.handleResponse(context, clientFirstMessage.getBytes(StandardCharsets.UTF_8));

        assertFalse(result.isComplete(), "Should not be complete after first message");
        assertNotNull(result.getChallenge(), "Should return a challenge");

        // Verify server-first-message format: r=<nonce>,s=<salt>,i=<iterations>
        String serverFirstMessage = new String(result.getChallenge(), StandardCharsets.UTF_8);
        assertTrue(serverFirstMessage.startsWith("r=" + clientNonce), "Nonce should include client nonce");
        assertTrue(serverFirstMessage.contains(",s="), "Should contain salt");
        assertTrue(serverFirstMessage.contains(",i="), "Should contain iterations");
    }

    @Test
    @DisplayName("SCRAM: Should reject unknown user")
    void testUnknownUser() {
        String clientNonce = generateNonce();
        String clientFirstMessage = "n,,n=unknownuser,r=" + clientNonce;

        SaslResult result = mechanism.handleResponse(context, clientFirstMessage.getBytes(StandardCharsets.UTF_8));

        assertTrue(result.isComplete());
        assertFalse(result.isSuccess());
    }

    @Test
    @DisplayName("SCRAM: Should reject empty response")
    void testEmptyResponse() {
        SaslResult result = mechanism.handleResponse(context, new byte[0]);

        assertTrue(result.isComplete());
        assertFalse(result.isSuccess());
    }

    @Test
    @DisplayName("SCRAM: Should reject null response")
    void testNullResponse() {
        SaslResult result = mechanism.handleResponse(context, null);

        assertTrue(result.isComplete());
        assertFalse(result.isSuccess());
    }

    @Test
    @DisplayName("SCRAM: Should reject malformed client-first-message")
    void testMalformedClientFirstMessage() {
        // Missing username
        String malformed = "n,,r=clientnonce";
        SaslResult result = mechanism.handleResponse(context, malformed.getBytes(StandardCharsets.UTF_8));

        assertTrue(result.isComplete());
        assertFalse(result.isSuccess());
    }

    @Test
    @DisplayName("SCRAM: Should reject channel binding request")
    void testChannelBindingNotSupported() {
        String clientNonce = generateNonce();
        String clientFirstMessage = "p=tls-unique,,n=guest,r=" + clientNonce;

        SaslResult result = mechanism.handleResponse(context, clientFirstMessage.getBytes(StandardCharsets.UTF_8));

        assertTrue(result.isComplete());
        assertFalse(result.isSuccess());
    }

    @Test
    @DisplayName("SCRAM: Should be applicable in any context")
    void testIsApplicable() {
        assertTrue(mechanism.isApplicable(context));
    }

    @Test
    @DisplayName("SCRAM: Should have higher priority than PLAIN")
    void testPriority() {
        PlainMechanism plain = new PlainMechanism((u, p) -> true);
        ScramMechanism scram = new ScramMechanism(u -> null);

        assertTrue(scram.getPriority() > plain.getPriority(),
                "SCRAM should have higher priority than PLAIN");
    }

    @Test
    @DisplayName("SCRAM: Credentials creation should produce valid stored key")
    void testCredentialsCreation() throws Exception {
        byte[] salt = new byte[16];
        random.nextBytes(salt);

        ScramMechanism.ScramCredentials creds =
                ScramMechanism.createCredentials("testpassword", salt, 4096);

        assertNotNull(creds.getStoredKey());
        assertNotNull(creds.getServerKey());
        assertEquals(salt, creds.getSalt());
        assertEquals(4096, creds.getIterations());
        assertNotNull(creds.getSaltBase64());

        // Verify salt is correctly base64 encoded
        byte[] decodedSalt = Base64.getDecoder().decode(creds.getSaltBase64());
        assertArrayEquals(salt, decodedSalt);
    }

    @Test
    @DisplayName("SCRAM: Same password with different salt produces different credentials")
    void testDifferentSaltProducesDifferentCredentials() throws Exception {
        byte[] salt1 = new byte[16];
        byte[] salt2 = new byte[16];
        random.nextBytes(salt1);
        random.nextBytes(salt2);

        ScramMechanism.ScramCredentials creds1 =
                ScramMechanism.createCredentials("password", salt1, 4096);
        ScramMechanism.ScramCredentials creds2 =
                ScramMechanism.createCredentials("password", salt2, 4096);

        assertFalse(java.util.Arrays.equals(creds1.getStoredKey(), creds2.getStoredKey()),
                "Different salts should produce different stored keys");
    }

    @Test
    @DisplayName("SCRAM: Same password with same salt produces same credentials")
    void testSameSaltProducesSameCredentials() throws Exception {
        byte[] salt = new byte[16];
        random.nextBytes(salt);

        ScramMechanism.ScramCredentials creds1 =
                ScramMechanism.createCredentials("password", salt.clone(), 4096);
        ScramMechanism.ScramCredentials creds2 =
                ScramMechanism.createCredentials("password", salt.clone(), 4096);

        assertArrayEquals(creds1.getStoredKey(), creds2.getStoredKey(),
                "Same password and salt should produce same stored key");
    }

    @Test
    @DisplayName("SCRAM: Different iterations produce different credentials")
    void testDifferentIterations() throws Exception {
        byte[] salt = new byte[16];
        random.nextBytes(salt);

        ScramMechanism.ScramCredentials creds1 =
                ScramMechanism.createCredentials("password", salt.clone(), 4096);
        ScramMechanism.ScramCredentials creds2 =
                ScramMechanism.createCredentials("password", salt.clone(), 8192);

        assertFalse(java.util.Arrays.equals(creds1.getStoredKey(), creds2.getStoredKey()),
                "Different iterations should produce different stored keys");
    }

    @Test
    @DisplayName("SCRAM: handleChallengeResponse rejects nonce mismatch")
    void testNonceMismatch() throws Exception {
        // First, do the initial exchange
        String clientNonce = generateNonce();
        String clientFirstMessage = "n,,n=guest,r=" + clientNonce;

        SaslResult result1 = mechanism.handleResponse(context, clientFirstMessage.getBytes(StandardCharsets.UTF_8));
        assertFalse(result1.isComplete());

        // Now try with wrong nonce
        String wrongNonce = generateNonce();
        String clientFinalMessage = "c=biws,r=" + wrongNonce + ",p=fakeproof";

        SaslResult result2 = mechanism.handleChallengeResponse(context,
                clientFinalMessage.getBytes(StandardCharsets.UTF_8));

        assertTrue(result2.isComplete());
        assertFalse(result2.isSuccess());
    }

    private String generateNonce() {
        byte[] nonceBytes = new byte[18];
        random.nextBytes(nonceBytes);
        return Base64.getEncoder().encodeToString(nonceBytes);
    }

    /**
     * Simple SaslContext implementation for testing.
     */
    private static class SimpleSaslContext implements SaslContext {
        private final Map<String, Object> attributes = new HashMap<>();
        private boolean tlsEnabled = false;

        @Override
        public void setAttribute(String key, Object value) {
            attributes.put(key, value);
        }

        @Override
        public <T> T getAttribute(String key, Class<T> type) {
            return type.cast(attributes.get(key));
        }

        @Override
        public void removeAttribute(String key) {
            attributes.remove(key);
        }

        @Override
        public boolean isTlsEnabled() {
            return tlsEnabled;
        }
    }
}
