package com.amqp.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

public class PasswordHasher {
    private static final String ALGORITHM = "SHA-256";
    private static final int SALT_LENGTH = 16;
    private static final SecureRandom RANDOM = new SecureRandom();

    public static String hash(String plainPassword) {
        try {
            byte[] salt = generateSalt();
            byte[] hash = hashWithSalt(plainPassword, salt);

            byte[] combined = new byte[salt.length + hash.length];
            System.arraycopy(salt, 0, combined, 0, salt.length);
            System.arraycopy(hash, 0, combined, salt.length, hash.length);

            return Base64.getEncoder().encodeToString(combined);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to hash password", e);
        }
    }

    public static boolean verify(String plainPassword, String hashedPassword) {
        try {
            byte[] combined = Base64.getDecoder().decode(hashedPassword);

            byte[] salt = new byte[SALT_LENGTH];
            byte[] storedHash = new byte[combined.length - SALT_LENGTH];

            System.arraycopy(combined, 0, salt, 0, SALT_LENGTH);
            System.arraycopy(combined, SALT_LENGTH, storedHash, 0, storedHash.length);

            byte[] computedHash = hashWithSalt(plainPassword, salt);

            return MessageDigest.isEqual(storedHash, computedHash);
        } catch (Exception e) {
            return false;
        }
    }

    private static byte[] generateSalt() {
        byte[] salt = new byte[SALT_LENGTH];
        RANDOM.nextBytes(salt);
        return salt;
    }

    private static byte[] hashWithSalt(String password, byte[] salt) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance(ALGORITHM);
        digest.update(salt);
        return digest.digest(password.getBytes(StandardCharsets.UTF_8));
    }
}
