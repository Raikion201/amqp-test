package com.amqp.security.sasl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * SASL ANONYMOUS authentication mechanism.
 *
 * Allows unauthenticated access with optional trace information.
 */
public class AnonymousMechanism implements SaslMechanism {

    private static final Logger log = LoggerFactory.getLogger(AnonymousMechanism.class);

    public static final String NAME = "ANONYMOUS";
    public static final String ANONYMOUS_USER = "anonymous";

    private final boolean enabled;

    public AnonymousMechanism() {
        this(true);
    }

    public AnonymousMechanism(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isApplicable(SaslContext context) {
        return enabled;
    }

    @Override
    public int getPriority() {
        return 1; // Lowest priority
    }

    @Override
    public SaslResult handleResponse(SaslContext context, byte[] initialResponse) {
        if (!enabled) {
            return SaslResult.authFailed("Anonymous access disabled");
        }

        // The initial response is optional trace information
        String traceInfo = null;
        if (initialResponse != null && initialResponse.length > 0) {
            traceInfo = new String(initialResponse, StandardCharsets.UTF_8);
            log.debug("ANONYMOUS authentication with trace: {}", traceInfo);
        } else {
            log.debug("ANONYMOUS authentication without trace");
        }

        log.info("ANONYMOUS authentication successful");
        return SaslResult.success(ANONYMOUS_USER);
    }

    @Override
    public SaslResult handleChallengeResponse(SaslContext context, byte[] response) {
        // ANONYMOUS doesn't use challenges
        return SaslResult.authFailed("Unexpected challenge response");
    }
}
