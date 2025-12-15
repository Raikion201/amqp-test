package com.amqp.security;

import at.favre.lib.crypto.bcrypt.BCrypt;

/**
 * Secure password hashing using bcrypt with configurable cost factor.
 * BCrypt is specifically designed for password hashing with:
 * - Adaptive cost factor (work factor) to stay resistant to brute-force attacks
 * - Built-in salt generation
 * - Constant-time comparison to prevent timing attacks
 */
public class PasswordHasher {
    // Cost factor (work factor): 12 = 2^12 iterations = 4096 iterations
    // Higher values increase security but also increase computation time
    // 12 is a good balance as of 2024
    private static final int BCRYPT_COST = 12;

    /**
     * Hash a plain text password using bcrypt with salt and cost factor.
     *
     * @param plainPassword the password to hash
     * @return the bcrypt hash string (includes salt and cost factor)
     */
    public static String hash(String plainPassword) {
        if (plainPassword == null || plainPassword.isEmpty()) {
            throw new IllegalArgumentException("Password cannot be null or empty");
        }
        return BCrypt.withDefaults().hashToString(BCRYPT_COST, plainPassword.toCharArray());
    }

    /**
     * Verify a plain text password against a bcrypt hash.
     * Uses constant-time comparison to prevent timing attacks.
     *
     * @param plainPassword the password to verify
     * @param hashedPassword the bcrypt hash to verify against
     * @return true if the password matches, false otherwise
     */
    public static boolean verify(String plainPassword, String hashedPassword) {
        if (plainPassword == null || hashedPassword == null) {
            return false;
        }

        try {
            BCrypt.Result result = BCrypt.verifyer().verify(plainPassword.toCharArray(), hashedPassword);
            return result.verified;
        } catch (Exception e) {
            // Invalid hash format or other error
            return false;
        }
    }
}
