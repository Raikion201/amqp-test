package com.amqp.security;

import at.favre.lib.crypto.bcrypt.BCrypt;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Secure password hashing using bcrypt with configurable cost factor.
 * BCrypt is specifically designed for password hashing with:
 * - Adaptive cost factor (work factor) to stay resistant to brute-force attacks
 * - Built-in salt generation
 * - Constant-time comparison to prevent timing attacks
 *
 * Also provides password complexity validation for security compliance.
 */
public class PasswordHasher {
    // Cost factor (work factor): 12 = 2^12 iterations = 4096 iterations
    // Higher values increase security but also increase computation time
    // 12 is a good balance as of 2024
    private static final int BCRYPT_COST = 12;

    // Password complexity requirements
    private static final int MIN_PASSWORD_LENGTH = 8;
    private static final int MAX_PASSWORD_LENGTH = 128;

    // Patterns for complexity validation
    private static final Pattern HAS_UPPERCASE = Pattern.compile("[A-Z]");
    private static final Pattern HAS_LOWERCASE = Pattern.compile("[a-z]");
    private static final Pattern HAS_DIGIT = Pattern.compile("[0-9]");
    private static final Pattern HAS_SPECIAL = Pattern.compile("[!@#$%^&*()_+\\-=\\[\\]{};':\"\\\\|,.<>/?`~]");

    // Common weak passwords to reject
    private static final String[] COMMON_WEAK_PASSWORDS = {
        "password", "12345678", "123456789", "qwerty123", "password1",
        "admin123", "letmein", "welcome1", "monkey123", "dragon123",
        "master123", "login123", "abc12345", "password123", "admin1234"
    };

    /**
     * Result of password complexity validation.
     */
    public static class ValidationResult {
        private final boolean valid;
        private final List<String> errors;

        public ValidationResult(boolean valid, List<String> errors) {
            this.valid = valid;
            this.errors = errors;
        }

        public boolean isValid() {
            return valid;
        }

        public List<String> getErrors() {
            return errors;
        }

        @Override
        public String toString() {
            if (valid) {
                return "Password is valid";
            }
            return "Password validation failed: " + String.join("; ", errors);
        }
    }

    /**
     * Validate password complexity requirements.
     *
     * Requirements:
     * - Minimum 8 characters, maximum 128 characters
     * - At least one uppercase letter (A-Z)
     * - At least one lowercase letter (a-z)
     * - At least one digit (0-9)
     * - At least one special character
     * - Not a commonly used weak password
     *
     * @param password the password to validate
     * @return ValidationResult with success status and any error messages
     */
    public static ValidationResult validateComplexity(String password) {
        List<String> errors = new ArrayList<>();

        if (password == null) {
            errors.add("Password cannot be null");
            return new ValidationResult(false, errors);
        }

        // Length checks
        if (password.length() < MIN_PASSWORD_LENGTH) {
            errors.add("Password must be at least " + MIN_PASSWORD_LENGTH + " characters");
        }
        if (password.length() > MAX_PASSWORD_LENGTH) {
            errors.add("Password cannot exceed " + MAX_PASSWORD_LENGTH + " characters");
        }

        // Complexity checks
        if (!HAS_UPPERCASE.matcher(password).find()) {
            errors.add("Password must contain at least one uppercase letter (A-Z)");
        }
        if (!HAS_LOWERCASE.matcher(password).find()) {
            errors.add("Password must contain at least one lowercase letter (a-z)");
        }
        if (!HAS_DIGIT.matcher(password).find()) {
            errors.add("Password must contain at least one digit (0-9)");
        }
        if (!HAS_SPECIAL.matcher(password).find()) {
            errors.add("Password must contain at least one special character (!@#$%^&*...)");
        }

        // Check for common weak passwords
        String lowerPassword = password.toLowerCase();
        for (String weak : COMMON_WEAK_PASSWORDS) {
            if (lowerPassword.equals(weak) || lowerPassword.contains(weak)) {
                errors.add("Password is too common or contains a commonly used weak password");
                break;
            }
        }

        return new ValidationResult(errors.isEmpty(), errors);
    }

    /**
     * Validate password complexity and throw exception if invalid.
     *
     * @param password the password to validate
     * @throws IllegalArgumentException if password doesn't meet complexity requirements
     */
    public static void requireComplexPassword(String password) {
        ValidationResult result = validateComplexity(password);
        if (!result.isValid()) {
            throw new IllegalArgumentException(result.toString());
        }
    }

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
     * Hash a password after validating complexity requirements.
     * Use this method when creating new user passwords.
     *
     * @param plainPassword the password to validate and hash
     * @return the bcrypt hash string
     * @throws IllegalArgumentException if password doesn't meet complexity requirements
     */
    public static String hashWithValidation(String plainPassword) {
        requireComplexPassword(plainPassword);
        return hash(plainPassword);
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
