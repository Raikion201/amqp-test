package com.amqp.security.sasl;

import com.amqp.security.tls.CertificateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.cert.X509Certificate;
import java.util.function.Function;

/**
 * SASL EXTERNAL authentication mechanism.
 *
 * Uses the client's TLS certificate for authentication.
 * The username is extracted from the certificate's Common Name or SAN.
 */
public class ExternalMechanism implements SaslMechanism {

    private static final Logger log = LoggerFactory.getLogger(ExternalMechanism.class);

    public static final String NAME = "EXTERNAL";

    private final Function<X509Certificate, String> usernameExtractor;
    private final Function<String, Boolean> userValidator;

    /**
     * Create with default username extraction (from certificate).
     */
    public ExternalMechanism() {
        this(CertificateManager::extractUsername, user -> true);
    }

    /**
     * Create with custom username extraction and validation.
     *
     * @param usernameExtractor function to extract username from certificate
     * @param userValidator function to validate the extracted username
     */
    public ExternalMechanism(Function<X509Certificate, String> usernameExtractor,
                             Function<String, Boolean> userValidator) {
        this.usernameExtractor = usernameExtractor;
        this.userValidator = userValidator;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isApplicable(SaslContext context) {
        // EXTERNAL only works when we have a client certificate
        return context.isTlsEnabled() && context.hasClientCertificate();
    }

    @Override
    public int getPriority() {
        return 100; // Highest priority when applicable
    }

    @Override
    public SaslResult handleResponse(SaslContext context, byte[] initialResponse) {
        if (!context.isTlsEnabled()) {
            return SaslResult.authFailed("TLS required for EXTERNAL");
        }

        X509Certificate cert = context.getClientCertificate();
        if (cert == null) {
            return SaslResult.authFailed("No client certificate");
        }

        // Verify certificate is valid
        if (!CertificateManager.isValid(cert)) {
            log.warn("Client certificate is expired or not yet valid");
            return SaslResult.authFailed("Certificate not valid");
        }

        // Extract username
        String username;
        try {
            username = usernameExtractor.apply(cert);
        } catch (Exception e) {
            log.error("Error extracting username from certificate", e);
            return SaslResult.systemError("Error extracting username");
        }

        if (username == null || username.isEmpty()) {
            log.warn("Could not extract username from certificate");
            return SaslResult.authFailed("No username in certificate");
        }

        // Check for authorization identity in response
        if (initialResponse != null && initialResponse.length > 0) {
            String authzid = new String(initialResponse).trim();
            if (!authzid.isEmpty() && !authzid.equals(username)) {
                // Client wants to authorize as a different user
                log.debug("EXTERNAL: authzid={} requested by authcid={}", authzid, username);
                // For now, we only allow authorizing as yourself
                return SaslResult.authFailed("Authorization as different user not allowed");
            }
        }

        // Validate the user
        try {
            if (!userValidator.apply(username)) {
                log.warn("EXTERNAL user validation failed: {}", username);
                return SaslResult.authFailed("User not authorized");
            }
        } catch (Exception e) {
            log.error("Error validating user", e);
            return SaslResult.systemError(e.getMessage());
        }

        log.info("EXTERNAL authentication successful: {}", username);
        return SaslResult.success(username);
    }

    @Override
    public SaslResult handleChallengeResponse(SaslContext context, byte[] response) {
        // EXTERNAL doesn't use challenges
        return SaslResult.authFailed("Unexpected challenge response");
    }
}
