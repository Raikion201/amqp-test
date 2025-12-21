package com.amqp.security.tls;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSession;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.*;

/**
 * Manages certificates for TLS operations.
 *
 * Provides utilities for loading, validating, and extracting information
 * from X.509 certificates.
 */
public class CertificateManager {

    private static final Logger log = LoggerFactory.getLogger(CertificateManager.class);

    /**
     * Load certificates from a keystore.
     */
    public static List<X509Certificate> loadCertificates(String keystorePath,
                                                          String password,
                                                          String type) throws Exception {
        List<X509Certificate> certs = new ArrayList<>();

        KeyStore keyStore = KeyStore.getInstance(type);
        try (InputStream is = new FileInputStream(keystorePath)) {
            keyStore.load(is, password != null ? password.toCharArray() : null);
        }

        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            Certificate cert = keyStore.getCertificate(alias);
            if (cert instanceof X509Certificate) {
                certs.add((X509Certificate) cert);
            }
        }

        return certs;
    }

    /**
     * Extract the client certificate from an SSL session.
     */
    public static X509Certificate getClientCertificate(SSLSession session) {
        try {
            Certificate[] peerCerts = session.getPeerCertificates();
            if (peerCerts != null && peerCerts.length > 0) {
                Certificate cert = peerCerts[0];
                if (cert instanceof X509Certificate) {
                    return (X509Certificate) cert;
                }
            }
        } catch (Exception e) {
            log.debug("No client certificate available", e);
        }
        return null;
    }

    /**
     * Extract the Common Name (CN) from a certificate's subject.
     */
    public static String getCommonName(X509Certificate cert) {
        if (cert == null) {
            return null;
        }

        String dn = cert.getSubjectX500Principal().getName();
        return extractCN(dn);
    }

    /**
     * Extract Common Name from a distinguished name string.
     */
    private static String extractCN(String dn) {
        if (dn == null) {
            return null;
        }

        // Parse the DN to extract CN
        String[] parts = dn.split(",");
        for (String part : parts) {
            String trimmed = part.trim();
            if (trimmed.toUpperCase().startsWith("CN=")) {
                return trimmed.substring(3).trim();
            }
        }
        return null;
    }

    /**
     * Extract the username from a client certificate.
     *
     * Tries to extract from:
     * 1. Subject Alternative Name (email)
     * 2. Common Name
     * 3. Subject DN
     */
    public static String extractUsername(X509Certificate cert) {
        if (cert == null) {
            return null;
        }

        // Try SAN email first
        try {
            Collection<List<?>> sans = cert.getSubjectAlternativeNames();
            if (sans != null) {
                for (List<?> san : sans) {
                    Integer type = (Integer) san.get(0);
                    if (type == 1) { // rfc822Name (email)
                        String email = (String) san.get(1);
                        // Extract username from email
                        int atIndex = email.indexOf('@');
                        if (atIndex > 0) {
                            return email.substring(0, atIndex);
                        }
                        return email;
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Error extracting SAN", e);
        }

        // Fall back to CN
        String cn = getCommonName(cert);
        if (cn != null && !cn.isEmpty()) {
            return cn;
        }

        // Last resort: use the subject DN
        return cert.getSubjectX500Principal().getName();
    }

    /**
     * Get the certificate chain from an SSL session.
     */
    public static X509Certificate[] getCertificateChain(SSLSession session) {
        try {
            Certificate[] peerCerts = session.getPeerCertificates();
            if (peerCerts != null) {
                X509Certificate[] chain = new X509Certificate[peerCerts.length];
                for (int i = 0; i < peerCerts.length; i++) {
                    if (peerCerts[i] instanceof X509Certificate) {
                        chain[i] = (X509Certificate) peerCerts[i];
                    }
                }
                return chain;
            }
        } catch (Exception e) {
            log.debug("No certificate chain available", e);
        }
        return new X509Certificate[0];
    }

    /**
     * Validate that a certificate is currently valid (not expired).
     */
    public static boolean isValid(X509Certificate cert) {
        if (cert == null) {
            return false;
        }

        try {
            cert.checkValidity();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Get the expiration date of a certificate.
     */
    public static Date getExpirationDate(X509Certificate cert) {
        return cert != null ? cert.getNotAfter() : null;
    }

    /**
     * Get days until certificate expiration.
     */
    public static long getDaysUntilExpiration(X509Certificate cert) {
        if (cert == null) {
            return -1;
        }

        Date expiry = cert.getNotAfter();
        long diff = expiry.getTime() - System.currentTimeMillis();
        return diff / (24 * 60 * 60 * 1000);
    }

    /**
     * Get certificate fingerprint (SHA-256).
     */
    public static String getFingerprint(X509Certificate cert) {
        if (cert == null) {
            return null;
        }

        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(cert.getEncoded());
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02X:", b & 0xff));
            }
            return sb.substring(0, sb.length() - 1);
        } catch (Exception e) {
            log.error("Error computing certificate fingerprint", e);
            return null;
        }
    }

    /**
     * Create a certificate info summary.
     */
    public static CertificateInfo getCertificateInfo(X509Certificate cert) {
        if (cert == null) {
            return null;
        }

        return new CertificateInfo(
                getCommonName(cert),
                cert.getSubjectX500Principal().getName(),
                cert.getIssuerX500Principal().getName(),
                cert.getNotBefore(),
                cert.getNotAfter(),
                cert.getSerialNumber().toString(16),
                getFingerprint(cert),
                isValid(cert)
        );
    }

    /**
     * Certificate information summary.
     */
    public static class CertificateInfo {
        private final String commonName;
        private final String subject;
        private final String issuer;
        private final Date notBefore;
        private final Date notAfter;
        private final String serialNumber;
        private final String fingerprint;
        private final boolean valid;

        public CertificateInfo(String commonName, String subject, String issuer,
                               Date notBefore, Date notAfter, String serialNumber,
                               String fingerprint, boolean valid) {
            this.commonName = commonName;
            this.subject = subject;
            this.issuer = issuer;
            this.notBefore = notBefore;
            this.notAfter = notAfter;
            this.serialNumber = serialNumber;
            this.fingerprint = fingerprint;
            this.valid = valid;
        }

        public String getCommonName() { return commonName; }
        public String getSubject() { return subject; }
        public String getIssuer() { return issuer; }
        public Date getNotBefore() { return notBefore; }
        public Date getNotAfter() { return notAfter; }
        public String getSerialNumber() { return serialNumber; }
        public String getFingerprint() { return fingerprint; }
        public boolean isValid() { return valid; }

        @Override
        public String toString() {
            return String.format("CertificateInfo{CN=%s, valid=%s, expires=%s}",
                    commonName, valid, notAfter);
        }
    }
}
