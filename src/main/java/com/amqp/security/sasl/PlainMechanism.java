package com.amqp.security.sasl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.function.BiFunction;

/**
 * SASL PLAIN authentication mechanism.
 *
 * The PLAIN mechanism transmits credentials as:
 * [authzid] NUL authcid NUL passwd
 *
 * Where authzid is optional, authcid is the username, and passwd is the password.
 */
public class PlainMechanism implements SaslMechanism {

    private static final Logger log = LoggerFactory.getLogger(PlainMechanism.class);

    public static final String NAME = "PLAIN";

    private final BiFunction<String, String, Boolean> authenticator;
    private final boolean requireTls;

    /**
     * Create with a custom authenticator function.
     * TLS is required by default for security.
     *
     * @param authenticator function that takes (username, password) and returns true if valid
     */
    public PlainMechanism(BiFunction<String, String, Boolean> authenticator) {
        this(authenticator, true);
    }

    /**
     * Create with a custom authenticator function and TLS requirement setting.
     *
     * @param authenticator function that takes (username, password) and returns true if valid
     * @param requireTls whether to require TLS for this mechanism (should be true in production)
     */
    public PlainMechanism(BiFunction<String, String, Boolean> authenticator, boolean requireTls) {
        this.authenticator = authenticator;
        this.requireTls = requireTls;
        if (!requireTls) {
            log.warn("PLAIN SASL mechanism configured WITHOUT TLS requirement - " +
                     "passwords will be transmitted in clear text! This is insecure for production.");
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isApplicable(SaslContext context) {
        // PLAIN transmits passwords in clear text - TLS is required for security
        if (requireTls && !context.isTlsEnabled()) {
            log.debug("PLAIN mechanism not applicable: TLS required but not enabled");
            return false;
        }
        return true;
    }

    @Override
    public int getPriority() {
        return 10; // Lower priority than SCRAM
    }

    @Override
    public SaslResult handleResponse(SaslContext context, byte[] initialResponse) {
        if (initialResponse == null || initialResponse.length == 0) {
            return SaslResult.authFailed("Empty response");
        }

        // Parse the PLAIN response
        // Format: [authzid] NUL authcid NUL passwd
        String response = new String(initialResponse, StandardCharsets.UTF_8);
        String[] parts = splitPlainResponse(response);

        if (parts == null || parts.length < 2) {
            return SaslResult.authFailed("Invalid PLAIN response format");
        }

        String authzid = parts[0]; // Authorization identity (optional)
        String authcid = parts[1]; // Authentication identity (username)
        String passwd = parts.length > 2 ? parts[2] : "";

        // Use authcid as the username
        String username = authcid;

        log.debug("PLAIN authentication for user: {}", username);

        // Authenticate
        try {
            if (authenticator.apply(username, passwd)) {
                log.info("PLAIN authentication successful for user: {}", username);
                return SaslResult.success(username);
            } else {
                log.warn("PLAIN authentication failed for user: {}", username);
                return SaslResult.authFailed("Authentication failed");
            }
        } catch (Exception e) {
            log.error("Error during PLAIN authentication", e);
            return SaslResult.systemError(e.getMessage());
        }
    }

    @Override
    public SaslResult handleChallengeResponse(SaslContext context, byte[] response) {
        // PLAIN doesn't use challenges
        return SaslResult.authFailed("Unexpected challenge response");
    }

    /**
     * Split a PLAIN response into its parts.
     */
    private String[] splitPlainResponse(String response) {
        // Find the NUL bytes
        int first = response.indexOf('\0');
        if (first < 0) {
            return null;
        }

        int second = response.indexOf('\0', first + 1);
        if (second < 0) {
            return null;
        }

        String authzid = response.substring(0, first);
        String authcid = response.substring(first + 1, second);
        String passwd = response.substring(second + 1);

        return new String[]{authzid, authcid, passwd};
    }

    /**
     * Create a PLAIN response for client use.
     */
    public static byte[] createResponse(String username, String password) {
        return createResponse("", username, password);
    }

    /**
     * Create a PLAIN response with authorization identity.
     */
    public static byte[] createResponse(String authzid, String authcid, String password) {
        String response = authzid + "\0" + authcid + "\0" + password;
        return response.getBytes(StandardCharsets.UTF_8);
    }
}
