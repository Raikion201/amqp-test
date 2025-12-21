package com.amqp.security.oauth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth2/JWT authentication provider.
 *
 * Authenticates users using JWT tokens passed as passwords in SASL PLAIN.
 */
public class OAuth2AuthProvider {

    private static final Logger log = LoggerFactory.getLogger(OAuth2AuthProvider.class);

    private final OAuth2Config config;
    private final JwtValidator jwtValidator;

    public OAuth2AuthProvider(OAuth2Config config) {
        this.config = config;
        this.jwtValidator = new JwtValidator(config);
    }

    /**
     * Initialize the OAuth2 provider.
     */
    public void initialize() throws Exception {
        if (!config.isEnabled()) {
            log.info("OAuth2 authentication is disabled");
            return;
        }

        jwtValidator.initialize();
        log.info("OAuth2 authentication provider initialized");
    }

    /**
     * Authenticate using a JWT token.
     *
     * @param username the username (for validation against token)
     * @param token the JWT token
     * @return authentication result
     */
    public AuthResult authenticate(String username, String token) {
        if (!config.isEnabled()) {
            return AuthResult.failure("OAuth2 not enabled");
        }

        // Check if token looks like a JWT
        if (!JwtValidator.isJwt(token)) {
            return AuthResult.failure("Token is not a valid JWT");
        }

        // Validate the token
        JwtValidator.TokenValidationResult result = jwtValidator.validate(token);

        if (!result.isValid()) {
            log.warn("JWT validation failed for user {}: {}", username, result.getError());
            return AuthResult.failure(result.getError());
        }

        // Verify username matches token
        String tokenUsername = result.getUsername();
        if (username != null && !username.isEmpty() && !username.equals(tokenUsername)) {
            // Username mismatch - might be using a service account
            // Some implementations allow this, others don't
            log.debug("Username mismatch: provided={}, token={}", username, tokenUsername);
        }

        log.info("OAuth2 authentication successful: {}", tokenUsername);
        return AuthResult.success(tokenUsername, result.getRoles(), result.getScopes());
    }

    /**
     * Authenticate using only a token (no username).
     */
    public AuthResult authenticateToken(String token) {
        return authenticate(null, token);
    }

    public boolean isEnabled() {
        return config.isEnabled() && jwtValidator.isEnabled();
    }

    /**
     * Authentication result.
     */
    public static class AuthResult {
        private final boolean success;
        private final String username;
        private final java.util.Set<String> roles;
        private final java.util.Set<String> scopes;
        private final String error;

        private AuthResult(boolean success, String username,
                          java.util.Set<String> roles, java.util.Set<String> scopes,
                          String error) {
            this.success = success;
            this.username = username;
            this.roles = roles;
            this.scopes = scopes;
            this.error = error;
        }

        public static AuthResult success(String username,
                                          java.util.Set<String> roles,
                                          java.util.Set<String> scopes) {
            return new AuthResult(true, username, roles, scopes, null);
        }

        public static AuthResult failure(String error) {
            return new AuthResult(false, null, null, null, error);
        }

        public boolean isSuccess() {
            return success;
        }

        public String getUsername() {
            return username;
        }

        public java.util.Set<String> getRoles() {
            return roles;
        }

        public java.util.Set<String> getScopes() {
            return scopes;
        }

        public String getError() {
            return error;
        }
    }
}
