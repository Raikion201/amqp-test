/*
 * Adapted from Apache ActiveMQ Artemis SASL tests.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.security.sasl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SASL PLAIN authentication mechanism.
 * Adapted from Apache ActiveMQ Artemis PlainSASLTest.
 */
@DisplayName("SASL PLAIN Mechanism Tests")
public class PlainMechanismTest {

    private Map<String, String> validUsers;
    private PlainMechanism mechanism;
    private SaslContext context;

    @BeforeEach
    void setUp() {
        validUsers = new HashMap<>();
        validUsers.put("guest", "guest");
        validUsers.put("admin", "adminpass");
        validUsers.put("testuser", "testpass");

        mechanism = new PlainMechanism((username, password) ->
                validUsers.containsKey(username) && validUsers.get(username).equals(password));

        context = new SimpleSaslContext();
    }

    @Test
    @DisplayName("PLAIN: Name should be 'PLAIN'")
    void testMechanismName() {
        assertEquals("PLAIN", mechanism.getName());
    }

    @Test
    @DisplayName("PLAIN: Should authenticate valid credentials")
    void testValidAuthentication() {
        byte[] response = PlainMechanism.createResponse("guest", "guest");
        SaslResult result = mechanism.handleResponse(context, response);

        assertTrue(result.isComplete());
        assertTrue(result.isSuccess());
        assertEquals("guest", result.getAuthenticatedUser());
    }

    @Test
    @DisplayName("PLAIN: Should reject invalid password")
    void testInvalidPassword() {
        byte[] response = PlainMechanism.createResponse("guest", "wrongpassword");
        SaslResult result = mechanism.handleResponse(context, response);

        assertTrue(result.isComplete());
        assertFalse(result.isSuccess());
    }

    @Test
    @DisplayName("PLAIN: Should reject unknown user")
    void testUnknownUser() {
        byte[] response = PlainMechanism.createResponse("unknownuser", "password");
        SaslResult result = mechanism.handleResponse(context, response);

        assertTrue(result.isComplete());
        assertFalse(result.isSuccess());
    }

    @Test
    @DisplayName("PLAIN: Should reject empty response")
    void testEmptyResponse() {
        SaslResult result = mechanism.handleResponse(context, new byte[0]);

        assertTrue(result.isComplete());
        assertFalse(result.isSuccess());
    }

    @Test
    @DisplayName("PLAIN: Should reject null response")
    void testNullResponse() {
        SaslResult result = mechanism.handleResponse(context, null);

        assertTrue(result.isComplete());
        assertFalse(result.isSuccess());
    }

    @Test
    @DisplayName("PLAIN: Should handle authorization identity")
    void testWithAuthorizationIdentity() {
        byte[] response = PlainMechanism.createResponse("admin", "guest", "guest");
        SaslResult result = mechanism.handleResponse(context, response);

        assertTrue(result.isComplete());
        assertTrue(result.isSuccess());
        assertEquals("guest", result.getAuthenticatedUser());
    }

    @Test
    @DisplayName("PLAIN: Should reject malformed response without NUL separators")
    void testMalformedResponseNoNul() {
        byte[] response = "guestguest".getBytes(StandardCharsets.UTF_8);
        SaslResult result = mechanism.handleResponse(context, response);

        assertTrue(result.isComplete());
        assertFalse(result.isSuccess());
    }

    @Test
    @DisplayName("PLAIN: Should reject malformed response with only one NUL")
    void testMalformedResponseOneNul() {
        byte[] response = "guest\0password".getBytes(StandardCharsets.UTF_8);
        SaslResult result = mechanism.handleResponse(context, response);

        assertTrue(result.isComplete());
        assertFalse(result.isSuccess());
    }

    @Test
    @DisplayName("PLAIN: Should handle empty password")
    void testEmptyPassword() {
        validUsers.put("nopass", "");
        byte[] response = PlainMechanism.createResponse("nopass", "");
        SaslResult result = mechanism.handleResponse(context, response);

        assertTrue(result.isComplete());
        assertTrue(result.isSuccess());
    }

    @Test
    @DisplayName("PLAIN: Should handle special characters in username")
    void testSpecialCharactersInUsername() {
        validUsers.put("user@domain.com", "password");
        byte[] response = PlainMechanism.createResponse("user@domain.com", "password");
        SaslResult result = mechanism.handleResponse(context, response);

        assertTrue(result.isComplete());
        assertTrue(result.isSuccess());
        assertEquals("user@domain.com", result.getAuthenticatedUser());
    }

    @Test
    @DisplayName("PLAIN: Should handle Unicode characters")
    void testUnicodeCharacters() {
        validUsers.put("用户", "密码");
        byte[] response = PlainMechanism.createResponse("用户", "密码");
        SaslResult result = mechanism.handleResponse(context, response);

        assertTrue(result.isComplete());
        assertTrue(result.isSuccess());
    }

    @Test
    @DisplayName("PLAIN: Should be applicable in any context")
    void testIsApplicable() {
        assertTrue(mechanism.isApplicable(context));
    }

    @Test
    @DisplayName("PLAIN: Should have lower priority than SCRAM")
    void testPriority() {
        PlainMechanism plain = new PlainMechanism((u, p) -> true);
        ScramMechanism scram = new ScramMechanism(u -> null);

        assertTrue(plain.getPriority() < scram.getPriority(),
                "PLAIN should have lower priority than SCRAM");
    }

    @Test
    @DisplayName("PLAIN: createResponse should produce correct format")
    void testCreateResponse() {
        byte[] response = PlainMechanism.createResponse("user", "pass");
        String decoded = new String(response, StandardCharsets.UTF_8);

        assertEquals("\0user\0pass", decoded);
    }

    @Test
    @DisplayName("PLAIN: createResponse with authzid should produce correct format")
    void testCreateResponseWithAuthzid() {
        byte[] response = PlainMechanism.createResponse("authz", "user", "pass");
        String decoded = new String(response, StandardCharsets.UTF_8);

        assertEquals("authz\0user\0pass", decoded);
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

        public void setTlsEnabled(boolean enabled) {
            this.tlsEnabled = enabled;
        }
    }
}
