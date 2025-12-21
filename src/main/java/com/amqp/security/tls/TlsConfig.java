package com.amqp.security.tls;

import java.util.Arrays;
import java.util.List;

/**
 * TLS Configuration for AMQP connections.
 *
 * Supports TLS 1.2 and 1.3 with configurable cipher suites and client authentication.
 */
public class TlsConfig {

    /**
     * Client authentication modes.
     */
    public enum ClientAuth {
        /** No client certificate required */
        NONE,
        /** Client certificate requested but not required */
        OPTIONAL,
        /** Client certificate required */
        REQUIRED
    }

    // Default TLS protocols
    public static final List<String> DEFAULT_PROTOCOLS = Arrays.asList("TLSv1.2", "TLSv1.3");

    // Recommended cipher suites for TLS 1.2 and 1.3
    public static final List<String> DEFAULT_CIPHERS = Arrays.asList(
            // TLS 1.3 ciphers
            "TLS_AES_256_GCM_SHA384",
            "TLS_AES_128_GCM_SHA256",
            "TLS_CHACHA20_POLY1305_SHA256",
            // TLS 1.2 ciphers
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
            "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"
    );

    private boolean enabled = false;
    private List<String> protocols = DEFAULT_PROTOCOLS;
    private List<String> ciphers = DEFAULT_CIPHERS;
    private ClientAuth clientAuth = ClientAuth.NONE;

    // Keystore configuration (server's certificate and private key)
    private String keystorePath;
    private String keystorePassword;
    private String keystoreType = "PKCS12";
    private String keyPassword;

    // Truststore configuration (for verifying client certificates)
    private String truststorePath;
    private String truststorePassword;
    private String truststoreType = "PKCS12";

    // Certificate revocation
    private boolean enableCrl = false;
    private String crlPath;
    private boolean enableOcsp = false;

    // Session configuration
    private int sessionCacheSize = 1000;
    private long sessionTimeout = 86400; // 24 hours in seconds

    public TlsConfig() {
    }

    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public boolean isEnabled() {
        return enabled;
    }

    public List<String> getProtocols() {
        return protocols;
    }

    public String[] getProtocolsArray() {
        return protocols.toArray(new String[0]);
    }

    public List<String> getCiphers() {
        return ciphers;
    }

    public String[] getCiphersArray() {
        return ciphers.toArray(new String[0]);
    }

    public ClientAuth getClientAuth() {
        return clientAuth;
    }

    public String getKeystorePath() {
        return keystorePath;
    }

    public String getKeystorePassword() {
        return keystorePassword;
    }

    public String getKeystoreType() {
        return keystoreType;
    }

    public String getKeyPassword() {
        return keyPassword != null ? keyPassword : keystorePassword;
    }

    public String getTruststorePath() {
        return truststorePath;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }

    public String getTruststoreType() {
        return truststoreType;
    }

    public boolean isEnableCrl() {
        return enableCrl;
    }

    public String getCrlPath() {
        return crlPath;
    }

    public boolean isEnableOcsp() {
        return enableOcsp;
    }

    public int getSessionCacheSize() {
        return sessionCacheSize;
    }

    public long getSessionTimeout() {
        return sessionTimeout;
    }

    public boolean isMutualTlsEnabled() {
        return clientAuth == ClientAuth.REQUIRED || clientAuth == ClientAuth.OPTIONAL;
    }

    // Setters
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setProtocols(List<String> protocols) {
        this.protocols = protocols;
    }

    public void setCiphers(List<String> ciphers) {
        this.ciphers = ciphers;
    }

    public void setClientAuth(ClientAuth clientAuth) {
        this.clientAuth = clientAuth;
    }

    public void setKeystorePath(String keystorePath) {
        this.keystorePath = keystorePath;
    }

    public void setKeystorePassword(String keystorePassword) {
        this.keystorePassword = keystorePassword;
    }

    public void setKeystoreType(String keystoreType) {
        this.keystoreType = keystoreType;
    }

    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    public void setTruststorePath(String truststorePath) {
        this.truststorePath = truststorePath;
    }

    public void setTruststorePassword(String truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    public void setTruststoreType(String truststoreType) {
        this.truststoreType = truststoreType;
    }

    public void setEnableCrl(boolean enableCrl) {
        this.enableCrl = enableCrl;
    }

    public void setCrlPath(String crlPath) {
        this.crlPath = crlPath;
    }

    public void setEnableOcsp(boolean enableOcsp) {
        this.enableOcsp = enableOcsp;
    }

    public void setSessionCacheSize(int sessionCacheSize) {
        this.sessionCacheSize = sessionCacheSize;
    }

    public void setSessionTimeout(long sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    /**
     * Builder for TlsConfig.
     */
    public static class Builder {
        private final TlsConfig config = new TlsConfig();

        public Builder enabled(boolean enabled) {
            config.enabled = enabled;
            return this;
        }

        public Builder protocols(String... protocols) {
            config.protocols = Arrays.asList(protocols);
            return this;
        }

        public Builder protocols(List<String> protocols) {
            config.protocols = protocols;
            return this;
        }

        public Builder ciphers(String... ciphers) {
            config.ciphers = Arrays.asList(ciphers);
            return this;
        }

        public Builder ciphers(List<String> ciphers) {
            config.ciphers = ciphers;
            return this;
        }

        public Builder clientAuth(ClientAuth clientAuth) {
            config.clientAuth = clientAuth;
            return this;
        }

        public Builder keystore(String path, String password) {
            config.keystorePath = path;
            config.keystorePassword = password;
            return this;
        }

        public Builder keystore(String path, String password, String type) {
            config.keystorePath = path;
            config.keystorePassword = password;
            config.keystoreType = type;
            return this;
        }

        public Builder keyPassword(String password) {
            config.keyPassword = password;
            return this;
        }

        public Builder truststore(String path, String password) {
            config.truststorePath = path;
            config.truststorePassword = password;
            return this;
        }

        public Builder truststore(String path, String password, String type) {
            config.truststorePath = path;
            config.truststorePassword = password;
            config.truststoreType = type;
            return this;
        }

        public Builder enableCrl(String crlPath) {
            config.enableCrl = true;
            config.crlPath = crlPath;
            return this;
        }

        public Builder enableOcsp() {
            config.enableOcsp = true;
            return this;
        }

        public Builder sessionCacheSize(int size) {
            config.sessionCacheSize = size;
            return this;
        }

        public Builder sessionTimeout(long timeoutSeconds) {
            config.sessionTimeout = timeoutSeconds;
            return this;
        }

        public TlsConfig build() {
            return config;
        }
    }

    @Override
    public String toString() {
        return String.format("TlsConfig{enabled=%s, protocols=%s, clientAuth=%s}",
                enabled, protocols, clientAuth);
    }
}
