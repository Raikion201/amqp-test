package com.amqp.security.oauth;

import com.nimbusds.jose.*;
import com.nimbusds.jose.jwk.*;
import com.nimbusds.jose.jwk.source.*;
import com.nimbusds.jose.proc.*;
import com.nimbusds.jwt.*;
import com.nimbusds.jwt.proc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.*;

/**
 * JWT token validator.
 *
 * Validates JWT tokens using JWKS or configured keys.
 */
public class JwtValidator {

    private static final Logger log = LoggerFactory.getLogger(JwtValidator.class);

    private final OAuth2Config config;
    private ConfigurableJWTProcessor<SecurityContext> jwtProcessor;
    private JWKSource<SecurityContext> keySource;

    public JwtValidator(OAuth2Config config) {
        this.config = config;
    }

    /**
     * Initialize the JWT validator.
     */
    public void initialize() throws Exception {
        if (!config.isEnabled()) {
            log.info("OAuth2/JWT authentication is disabled");
            return;
        }

        jwtProcessor = new DefaultJWTProcessor<>();

        // Set up key source
        if (config.useJwks()) {
            URL jwksUrl = new URL(config.getJwksUrl());
            keySource = new RemoteJWKSet<>(jwksUrl);
            log.info("JWT validator using JWKS from: {}", config.getJwksUrl());
        } else {
            // No key source configured - tokens will fail validation
            log.warn("No JWKS URL configured, JWT validation will fail");
            keySource = (jwkSelector, context) -> Collections.emptyList();
        }

        // Set up key selector
        JWSKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(
                getAllowedJWSAlgorithms(),
                keySource
        );
        jwtProcessor.setJWSKeySelector(keySelector);

        // Set up claims verification
        jwtProcessor.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<>(
                new JWTClaimsSet.Builder()
                        .issuer(config.getIssuer())
                        .audience(config.getAudience())
                        .build(),
                new HashSet<>(Arrays.asList("sub", "iat", "exp"))
        ));

        log.info("JWT validator initialized");
    }

    private Set<JWSAlgorithm> getAllowedJWSAlgorithms() {
        Set<JWSAlgorithm> algorithms = new HashSet<>();
        for (String alg : config.getAllowedAlgorithms()) {
            algorithms.add(JWSAlgorithm.parse(alg));
        }
        return algorithms;
    }

    /**
     * Validate a JWT token.
     *
     * @param token the JWT token string
     * @return the validation result
     */
    public TokenValidationResult validate(String token) {
        if (!config.isEnabled() || jwtProcessor == null) {
            return TokenValidationResult.failure("JWT validation is not enabled");
        }

        try {
            // Parse and validate the token
            JWTClaimsSet claims = jwtProcessor.process(token, null);

            // Extract username
            String username = claims.getStringClaim(config.getUsernameClaim());
            if (username == null || username.isEmpty()) {
                return TokenValidationResult.failure("No username claim in token");
            }

            // Extract roles/scopes
            Set<String> roles = extractRoles(claims);
            Set<String> scopes = extractScopes(claims);

            log.debug("JWT validated for user: {}", username);

            return TokenValidationResult.success(username, roles, scopes, claims);

        } catch (BadJOSEException e) {
            log.warn("JWT validation failed: {}", e.getMessage());
            return TokenValidationResult.failure("Token validation failed: " + e.getMessage());
        } catch (JOSEException e) {
            log.error("JWT processing error", e);
            return TokenValidationResult.failure("Token processing error");
        } catch (Exception e) {
            log.error("Unexpected error validating JWT", e);
            return TokenValidationResult.failure("Validation error");
        }
    }

    private Set<String> extractRoles(JWTClaimsSet claims) {
        Set<String> roles = new HashSet<>();
        try {
            Object rolesObj = claims.getClaim(config.getRolesClaim());
            if (rolesObj instanceof List) {
                for (Object role : (List<?>) rolesObj) {
                    roles.add(config.getRolePrefix() + role.toString().toUpperCase());
                }
            } else if (rolesObj instanceof String) {
                for (String role : ((String) rolesObj).split("[,\\s]+")) {
                    roles.add(config.getRolePrefix() + role.toUpperCase());
                }
            }
        } catch (Exception e) {
            log.debug("Error extracting roles from token", e);
        }
        return roles;
    }

    private Set<String> extractScopes(JWTClaimsSet claims) {
        Set<String> scopes = new HashSet<>();
        try {
            Object scopeObj = claims.getClaim(config.getScopesClaim());
            if (scopeObj instanceof List) {
                for (Object scope : (List<?>) scopeObj) {
                    scopes.add(scope.toString());
                }
            } else if (scopeObj instanceof String) {
                scopes.addAll(Arrays.asList(((String) scopeObj).split("\\s+")));
            }
        } catch (Exception e) {
            log.debug("Error extracting scopes from token", e);
        }
        return scopes;
    }

    /**
     * Check if a token looks like a JWT.
     */
    public static boolean isJwt(String token) {
        if (token == null || token.isEmpty()) {
            return false;
        }
        // JWTs have three base64-encoded parts separated by dots
        String[] parts = token.split("\\.");
        return parts.length == 3;
    }

    public boolean isEnabled() {
        return config.isEnabled() && jwtProcessor != null;
    }

    /**
     * Result of token validation.
     */
    public static class TokenValidationResult {
        private final boolean valid;
        private final String username;
        private final Set<String> roles;
        private final Set<String> scopes;
        private final JWTClaimsSet claims;
        private final String error;

        private TokenValidationResult(boolean valid, String username, Set<String> roles,
                                       Set<String> scopes, JWTClaimsSet claims, String error) {
            this.valid = valid;
            this.username = username;
            this.roles = roles != null ? roles : Collections.emptySet();
            this.scopes = scopes != null ? scopes : Collections.emptySet();
            this.claims = claims;
            this.error = error;
        }

        public static TokenValidationResult success(String username, Set<String> roles,
                                                     Set<String> scopes, JWTClaimsSet claims) {
            return new TokenValidationResult(true, username, roles, scopes, claims, null);
        }

        public static TokenValidationResult failure(String error) {
            return new TokenValidationResult(false, null, null, null, null, error);
        }

        public boolean isValid() {
            return valid;
        }

        public String getUsername() {
            return username;
        }

        public Set<String> getRoles() {
            return roles;
        }

        public Set<String> getScopes() {
            return scopes;
        }

        public JWTClaimsSet getClaims() {
            return claims;
        }

        public String getError() {
            return error;
        }

        public boolean hasScope(String scope) {
            return scopes.contains(scope);
        }

        public boolean hasRole(String role) {
            return roles.contains(role);
        }
    }
}
