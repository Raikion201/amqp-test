package com.amqp.security.sasl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * SASL SCRAM-SHA-256 authentication mechanism.
 *
 * Implements Salted Challenge Response Authentication Mechanism (SCRAM)
 * with SHA-256 as per RFC 5802 and RFC 7677.
 */
public class ScramMechanism implements SaslMechanism {

    private static final Logger log = LoggerFactory.getLogger(ScramMechanism.class);

    public static final String NAME = "SCRAM-SHA-256";
    private static final String HASH_ALGORITHM = "SHA-256";
    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final int DEFAULT_ITERATIONS = 4096;
    private static final int NONCE_LENGTH = 24;

    private final Function<String, ScramCredentials> credentialsLookup;
    private final SecureRandom random = new SecureRandom();

    /**
     * Create with a credentials lookup function.
     *
     * @param credentialsLookup function that takes username and returns stored credentials
     */
    public ScramMechanism(Function<String, ScramCredentials> credentialsLookup) {
        this.credentialsLookup = credentialsLookup;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isApplicable(SaslContext context) {
        return true; // SCRAM can be used with or without TLS
    }

    @Override
    public int getPriority() {
        return 50; // Higher priority than PLAIN
    }

    @Override
    public SaslResult handleResponse(SaslContext context, byte[] initialResponse) {
        if (initialResponse == null || initialResponse.length == 0) {
            return SaslResult.authFailed("Empty response");
        }

        try {
            // Parse client-first-message
            String clientFirstMessage = new String(initialResponse, StandardCharsets.UTF_8);
            Map<String, String> attrs = parseAttributes(clientFirstMessage);

            // Get the gs2-header (channel binding)
            String gs2Header = clientFirstMessage.substring(0, clientFirstMessage.indexOf(',') + 1);
            if (clientFirstMessage.startsWith("n,,")) {
                gs2Header = "n,,";
            } else if (clientFirstMessage.startsWith("y,,")) {
                gs2Header = "y,,";
            } else if (clientFirstMessage.startsWith("p=")) {
                // Channel binding not supported yet
                return SaslResult.authFailed("Channel binding not supported");
            }

            String username = attrs.get("n");
            String clientNonce = attrs.get("r");

            if (username == null || clientNonce == null) {
                return SaslResult.authFailed("Invalid client-first-message");
            }

            log.debug("SCRAM authentication for user: {}", username);

            // Look up credentials
            ScramCredentials credentials = credentialsLookup.apply(username);
            if (credentials == null) {
                // Don't reveal that user doesn't exist
                return SaslResult.authFailed("Authentication failed");
            }

            // Generate server nonce
            byte[] nonceBytes = new byte[NONCE_LENGTH];
            random.nextBytes(nonceBytes);
            String serverNonce = clientNonce + Base64.getEncoder().encodeToString(nonceBytes);

            // Create server-first-message
            String serverFirstMessage = String.format("r=%s,s=%s,i=%d",
                    serverNonce,
                    credentials.getSaltBase64(),
                    credentials.getIterations());

            // Store state for next step
            String clientFirstMessageBare = clientFirstMessage.substring(gs2Header.length());
            context.setAttribute("scram.username", username);
            context.setAttribute("scram.credentials", credentials);
            context.setAttribute("scram.clientFirstMessageBare", clientFirstMessageBare);
            context.setAttribute("scram.serverFirstMessage", serverFirstMessage);
            context.setAttribute("scram.clientNonce", clientNonce);
            context.setAttribute("scram.serverNonce", serverNonce);
            context.setAttribute("scram.gs2Header", gs2Header);

            return SaslResult.challenge(serverFirstMessage.getBytes(StandardCharsets.UTF_8));

        } catch (Exception e) {
            log.error("Error in SCRAM client-first-message", e);
            return SaslResult.systemError(e.getMessage());
        }
    }

    @Override
    public SaslResult handleChallengeResponse(SaslContext context, byte[] response) {
        if (response == null || response.length == 0) {
            return SaslResult.authFailed("Empty response");
        }

        try {
            // Parse client-final-message
            String clientFinalMessage = new String(response, StandardCharsets.UTF_8);
            Map<String, String> attrs = parseAttributes(clientFinalMessage);

            String channelBinding = attrs.get("c");
            String nonce = attrs.get("r");
            String proof = attrs.get("p");

            if (channelBinding == null || nonce == null || proof == null) {
                return SaslResult.authFailed("Invalid client-final-message");
            }

            // Retrieve stored state
            String username = context.getAttribute("scram.username", String.class);
            ScramCredentials credentials = context.getAttribute("scram.credentials", ScramCredentials.class);
            String clientFirstMessageBare = context.getAttribute("scram.clientFirstMessageBare", String.class);
            String serverFirstMessage = context.getAttribute("scram.serverFirstMessage", String.class);
            String serverNonce = context.getAttribute("scram.serverNonce", String.class);

            // Verify nonce
            if (!nonce.equals(serverNonce)) {
                return SaslResult.authFailed("Nonce mismatch");
            }

            // Calculate the auth message
            String clientFinalMessageWithoutProof = "c=" + channelBinding + ",r=" + nonce;
            String authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;

            // Verify the proof
            byte[] storedKey = credentials.getStoredKey();
            byte[] clientSignature = hmac(storedKey, authMessage.getBytes(StandardCharsets.UTF_8));
            byte[] clientProof = Base64.getDecoder().decode(proof);
            byte[] clientKey = xor(clientProof, clientSignature);
            byte[] computedStoredKey = hash(clientKey);

            if (!MessageDigest.isEqual(storedKey, computedStoredKey)) {
                log.warn("SCRAM authentication failed for user: {}", username);
                return SaslResult.authFailed("Authentication failed");
            }

            log.info("SCRAM authentication successful for user: {}", username);

            // Clear state
            context.removeAttribute("scram.username");
            context.removeAttribute("scram.credentials");
            context.removeAttribute("scram.clientFirstMessageBare");
            context.removeAttribute("scram.serverFirstMessage");
            context.removeAttribute("scram.clientNonce");
            context.removeAttribute("scram.serverNonce");
            context.removeAttribute("scram.gs2Header");

            return SaslResult.success(username);

        } catch (Exception e) {
            log.error("Error in SCRAM client-final-message", e);
            return SaslResult.systemError(e.getMessage());
        }
    }

    private Map<String, String> parseAttributes(String message) {
        Map<String, String> attrs = new HashMap<>();
        String[] parts = message.split(",");
        for (String part : parts) {
            int eq = part.indexOf('=');
            if (eq > 0) {
                String key = part.substring(0, eq);
                String value = part.substring(eq + 1);
                attrs.put(key, value);
            }
        }
        return attrs;
    }

    private byte[] hash(byte[] data) throws Exception {
        MessageDigest md = MessageDigest.getInstance(HASH_ALGORITHM);
        return md.digest(data);
    }

    private byte[] hmac(byte[] key, byte[] data) throws Exception {
        Mac mac = Mac.getInstance(HMAC_ALGORITHM);
        mac.init(new SecretKeySpec(key, HMAC_ALGORITHM));
        return mac.doFinal(data);
    }

    private byte[] xor(byte[] a, byte[] b) {
        byte[] result = new byte[a.length];
        for (int i = 0; i < a.length; i++) {
            result[i] = (byte) (a[i] ^ b[i]);
        }
        return result;
    }

    /**
     * Create SCRAM credentials for a password.
     */
    public static ScramCredentials createCredentials(String password, byte[] salt, int iterations)
            throws Exception {
        // Derive salted password using PBKDF2
        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, 256);
        byte[] saltedPassword = factory.generateSecret(spec).getEncoded();

        // Calculate ClientKey
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(saltedPassword, "HmacSHA256"));
        byte[] clientKey = mac.doFinal("Client Key".getBytes(StandardCharsets.UTF_8));

        // Calculate StoredKey
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] storedKey = md.digest(clientKey);

        // Calculate ServerKey
        mac.init(new SecretKeySpec(saltedPassword, "HmacSHA256"));
        byte[] serverKey = mac.doFinal("Server Key".getBytes(StandardCharsets.UTF_8));

        return new ScramCredentials(salt, iterations, storedKey, serverKey);
    }

    /**
     * SCRAM credentials stored on the server.
     */
    public static class ScramCredentials {
        private final byte[] salt;
        private final int iterations;
        private final byte[] storedKey;
        private final byte[] serverKey;

        public ScramCredentials(byte[] salt, int iterations, byte[] storedKey, byte[] serverKey) {
            this.salt = salt;
            this.iterations = iterations;
            this.storedKey = storedKey;
            this.serverKey = serverKey;
        }

        public byte[] getSalt() {
            return salt;
        }

        public String getSaltBase64() {
            return Base64.getEncoder().encodeToString(salt);
        }

        public int getIterations() {
            return iterations;
        }

        public byte[] getStoredKey() {
            return storedKey;
        }

        public byte[] getServerKey() {
            return serverKey;
        }
    }
}
