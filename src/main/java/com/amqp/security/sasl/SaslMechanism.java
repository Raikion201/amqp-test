package com.amqp.security.sasl;

/**
 * Interface for SASL authentication mechanisms.
 */
public interface SaslMechanism {

    /**
     * Get the name of this mechanism (e.g., "PLAIN", "SCRAM-SHA-256").
     */
    String getName();

    /**
     * Check if this mechanism is applicable given the current context.
     * For example, EXTERNAL only works with TLS client certificates.
     */
    boolean isApplicable(SaslContext context);

    /**
     * Handle the initial client response.
     *
     * @param context the SASL context
     * @param initialResponse the client's initial response (may be empty)
     * @return the authentication result
     */
    SaslResult handleResponse(SaslContext context, byte[] initialResponse);

    /**
     * Handle a client response to a challenge.
     *
     * @param context the SASL context
     * @param response the client's response to the challenge
     * @return the authentication result
     */
    SaslResult handleChallengeResponse(SaslContext context, byte[] response);

    /**
     * Get the priority of this mechanism (higher = more preferred).
     */
    default int getPriority() {
        return 0;
    }

    /**
     * Result of a SASL authentication step.
     */
    class SaslResult {
        private final SaslOutcome outcome;
        private final byte[] challenge;
        private final String username;
        private final String errorMessage;

        private SaslResult(SaslOutcome outcome, byte[] challenge,
                          String username, String errorMessage) {
            this.outcome = outcome;
            this.challenge = challenge;
            this.username = username;
            this.errorMessage = errorMessage;
        }

        public static SaslResult success(String username) {
            return new SaslResult(SaslOutcome.OK, null, username, null);
        }

        public static SaslResult challenge(byte[] challenge) {
            return new SaslResult(SaslOutcome.CONTINUE, challenge, null, null);
        }

        public static SaslResult authFailed(String message) {
            return new SaslResult(SaslOutcome.AUTH, null, null, message);
        }

        public static SaslResult systemError(String message) {
            return new SaslResult(SaslOutcome.SYS, null, null, message);
        }

        public SaslOutcome getOutcome() {
            return outcome;
        }

        public byte[] getChallenge() {
            return challenge;
        }

        public String getUsername() {
            return username;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public boolean isComplete() {
            return outcome != SaslOutcome.CONTINUE;
        }

        public boolean isSuccess() {
            return outcome == SaslOutcome.OK;
        }
    }

    /**
     * SASL outcome codes.
     */
    enum SaslOutcome {
        /** Authentication successful */
        OK(0),
        /** Authentication failed */
        AUTH(1),
        /** System error */
        SYS(2),
        /** System permanent error */
        SYS_PERM(3),
        /** System temporary error */
        SYS_TEMP(4),
        /** Continue with next challenge/response */
        CONTINUE(-1);

        private final int code;

        SaslOutcome(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static SaslOutcome fromCode(int code) {
            for (SaslOutcome outcome : values()) {
                if (outcome.code == code) {
                    return outcome;
                }
            }
            return SYS;
        }
    }
}
