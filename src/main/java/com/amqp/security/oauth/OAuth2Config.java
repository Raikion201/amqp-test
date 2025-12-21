package com.amqp.security.oauth;

import java.util.Arrays;
import java.util.List;

/**
 * OAuth2/JWT authentication configuration.
 */
public class OAuth2Config {

    private boolean enabled = false;

    // Token validation
    private String issuer;
    private String audience;
    private List<String> allowedAlgorithms = Arrays.asList("RS256", "RS384", "RS512", "ES256", "ES384", "ES512");

    // JWKS (JSON Web Key Set) for key retrieval
    private String jwksUrl;
    private long jwksCacheTime = 3600; // seconds

    // Introspection endpoint (for opaque tokens)
    private String introspectionUrl;
    private String clientId;
    private String clientSecret;

    // Claim mappings
    private String usernameClaim = "sub";
    private String rolesClaim = "roles";
    private String scopesClaim = "scope";
    private String rolePrefix = "ROLE_";

    // Token settings
    private long clockSkew = 60; // seconds
    private boolean requireHttps = true;

    public OAuth2Config() {
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public boolean isEnabled() {
        return enabled;
    }

    public String getIssuer() {
        return issuer;
    }

    public String getAudience() {
        return audience;
    }

    public List<String> getAllowedAlgorithms() {
        return allowedAlgorithms;
    }

    public String getJwksUrl() {
        return jwksUrl;
    }

    public long getJwksCacheTime() {
        return jwksCacheTime;
    }

    public String getIntrospectionUrl() {
        return introspectionUrl;
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getUsernameClaim() {
        return usernameClaim;
    }

    public String getRolesClaim() {
        return rolesClaim;
    }

    public String getScopesClaim() {
        return scopesClaim;
    }

    public String getRolePrefix() {
        return rolePrefix;
    }

    public long getClockSkew() {
        return clockSkew;
    }

    public boolean isRequireHttps() {
        return requireHttps;
    }

    public boolean useJwks() {
        return jwksUrl != null && !jwksUrl.isEmpty();
    }

    public boolean useIntrospection() {
        return introspectionUrl != null && !introspectionUrl.isEmpty();
    }

    // Setters
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setIssuer(String issuer) {
        this.issuer = issuer;
    }

    public void setAudience(String audience) {
        this.audience = audience;
    }

    public void setAllowedAlgorithms(List<String> allowedAlgorithms) {
        this.allowedAlgorithms = allowedAlgorithms;
    }

    public void setJwksUrl(String jwksUrl) {
        this.jwksUrl = jwksUrl;
    }

    public void setJwksCacheTime(long jwksCacheTime) {
        this.jwksCacheTime = jwksCacheTime;
    }

    public void setIntrospectionUrl(String introspectionUrl) {
        this.introspectionUrl = introspectionUrl;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public void setUsernameClaim(String usernameClaim) {
        this.usernameClaim = usernameClaim;
    }

    public void setRolesClaim(String rolesClaim) {
        this.rolesClaim = rolesClaim;
    }

    public void setScopesClaim(String scopesClaim) {
        this.scopesClaim = scopesClaim;
    }

    public void setRolePrefix(String rolePrefix) {
        this.rolePrefix = rolePrefix;
    }

    public void setClockSkew(long clockSkew) {
        this.clockSkew = clockSkew;
    }

    public void setRequireHttps(boolean requireHttps) {
        this.requireHttps = requireHttps;
    }

    public static class Builder {
        private final OAuth2Config config = new OAuth2Config();

        public Builder enabled(boolean enabled) {
            config.enabled = enabled;
            return this;
        }

        public Builder issuer(String issuer) {
            config.issuer = issuer;
            return this;
        }

        public Builder audience(String audience) {
            config.audience = audience;
            return this;
        }

        public Builder allowedAlgorithms(String... algorithms) {
            config.allowedAlgorithms = Arrays.asList(algorithms);
            return this;
        }

        public Builder jwksUrl(String url) {
            config.jwksUrl = url;
            return this;
        }

        public Builder jwksCacheTime(long seconds) {
            config.jwksCacheTime = seconds;
            return this;
        }

        public Builder introspection(String url, String clientId, String clientSecret) {
            config.introspectionUrl = url;
            config.clientId = clientId;
            config.clientSecret = clientSecret;
            return this;
        }

        public Builder claims(String username, String roles, String scopes) {
            config.usernameClaim = username;
            config.rolesClaim = roles;
            config.scopesClaim = scopes;
            return this;
        }

        public Builder clockSkew(long seconds) {
            config.clockSkew = seconds;
            return this;
        }

        public Builder requireHttps(boolean require) {
            config.requireHttps = require;
            return this;
        }

        public OAuth2Config build() {
            return config;
        }
    }
}
