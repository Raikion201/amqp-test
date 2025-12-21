package com.amqp.security.tls;

import io.netty.handler.ssl.*;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;

/**
 * Builder for creating Netty SslContext from TlsConfig.
 */
public class TlsContextBuilder {

    private static final Logger log = LoggerFactory.getLogger(TlsContextBuilder.class);

    private final TlsConfig config;

    public TlsContextBuilder(TlsConfig config) {
        this.config = config;
    }

    /**
     * Build an SslContext for server-side TLS.
     */
    public SslContext buildServerContext() throws Exception {
        if (!config.isEnabled()) {
            throw new IllegalStateException("TLS is not enabled");
        }

        // Load keystore
        KeyManagerFactory keyManagerFactory = loadKeyManagerFactory();

        // Build the context
        SslContextBuilder builder = SslContextBuilder.forServer(keyManagerFactory);

        // Configure protocols and ciphers
        builder.protocols(config.getProtocolsArray());
        builder.ciphers(Arrays.asList(config.getCiphersArray()));

        // Configure client authentication
        switch (config.getClientAuth()) {
            case REQUIRED:
                builder.clientAuth(ClientAuth.REQUIRE);
                break;
            case OPTIONAL:
                builder.clientAuth(ClientAuth.OPTIONAL);
                break;
            case NONE:
            default:
                builder.clientAuth(ClientAuth.NONE);
                break;
        }

        // Load truststore for client certificate verification
        if (config.isMutualTlsEnabled() && config.getTruststorePath() != null) {
            TrustManagerFactory trustManagerFactory = loadTrustManagerFactory();
            builder.trustManager(trustManagerFactory);
        }

        // Session configuration
        builder.sessionCacheSize(config.getSessionCacheSize());
        builder.sessionTimeout(config.getSessionTimeout());

        log.info("Built server SSL context with protocols={}, clientAuth={}",
                config.getProtocols(), config.getClientAuth());

        return builder.build();
    }

    /**
     * Build an SslContext for client-side TLS.
     */
    public SslContext buildClientContext() throws Exception {
        SslContextBuilder builder = SslContextBuilder.forClient();

        // Configure protocols and ciphers
        builder.protocols(config.getProtocolsArray());
        builder.ciphers(Arrays.asList(config.getCiphersArray()));

        // Load truststore for server certificate verification
        if (config.getTruststorePath() != null) {
            TrustManagerFactory trustManagerFactory = loadTrustManagerFactory();
            builder.trustManager(trustManagerFactory);
        } else {
            // For testing only - trust all certificates
            log.warn("No truststore configured, using insecure trust manager");
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        }

        // Load client certificate if mTLS is enabled
        if (config.getKeystorePath() != null) {
            KeyManagerFactory keyManagerFactory = loadKeyManagerFactory();
            builder.keyManager(keyManagerFactory);
        }

        return builder.build();
    }

    private KeyManagerFactory loadKeyManagerFactory() throws Exception {
        String keystorePath = config.getKeystorePath();
        String keystorePassword = config.getKeystorePassword();
        String keystoreType = config.getKeystoreType();
        String keyPassword = config.getKeyPassword();

        if (keystorePath == null) {
            throw new IllegalStateException("Keystore path is required");
        }

        KeyStore keyStore = KeyStore.getInstance(keystoreType);
        try (InputStream is = new FileInputStream(keystorePath)) {
            keyStore.load(is, keystorePassword != null ? keystorePassword.toCharArray() : null);
        }

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword != null ? keyPassword.toCharArray() : null);

        log.debug("Loaded keystore from {}", keystorePath);
        return kmf;
    }

    private TrustManagerFactory loadTrustManagerFactory() throws Exception {
        String truststorePath = config.getTruststorePath();
        String truststorePassword = config.getTruststorePassword();
        String truststoreType = config.getTruststoreType();

        if (truststorePath == null) {
            throw new IllegalStateException("Truststore path is required");
        }

        KeyStore trustStore = KeyStore.getInstance(truststoreType);
        try (InputStream is = new FileInputStream(truststorePath)) {
            trustStore.load(is, truststorePassword != null ? truststorePassword.toCharArray() : null);
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        log.debug("Loaded truststore from {}", truststorePath);
        return tmf;
    }

    /**
     * Create a simple self-signed SSL context for testing.
     */
    public static SslContext createSelfSignedContext() throws Exception {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        return SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
                .protocols("TLSv1.2", "TLSv1.3")
                .build();
    }

    /**
     * Validate that the configuration is complete for TLS.
     */
    public void validate() {
        if (!config.isEnabled()) {
            return;
        }

        if (config.getKeystorePath() == null || config.getKeystorePath().isEmpty()) {
            throw new IllegalStateException("Keystore path is required when TLS is enabled");
        }

        if (config.getKeystorePassword() == null) {
            throw new IllegalStateException("Keystore password is required when TLS is enabled");
        }

        if (config.isMutualTlsEnabled()) {
            if (config.getTruststorePath() == null || config.getTruststorePath().isEmpty()) {
                throw new IllegalStateException("Truststore path is required for mutual TLS");
            }
        }

        log.debug("TLS configuration validated successfully");
    }
}
