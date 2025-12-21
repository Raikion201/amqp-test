package com.amqp.security.ldap;

import com.unboundid.ldap.sdk.*;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustAllTrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocketFactory;
import java.util.*;

/**
 * LDAP authentication provider.
 *
 * Authenticates users against an LDAP directory and retrieves their groups.
 */
public class LdapAuthProvider implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LdapAuthProvider.class);

    private final LdapConfig config;
    private LDAPConnectionPool connectionPool;

    public LdapAuthProvider(LdapConfig config) {
        this.config = config;
    }

    /**
     * Initialize the LDAP connection pool.
     */
    public void initialize() throws LDAPException {
        if (!config.isEnabled()) {
            log.info("LDAP authentication is disabled");
            return;
        }

        try {
            LDAPConnection connection = createConnection();
            connectionPool = new LDAPConnectionPool(connection, config.getPoolSize());
            log.info("LDAP connection pool initialized: {}", config.getServerUrl());
        } catch (Exception e) {
            log.error("Failed to initialize LDAP connection pool", e);
            throw new LDAPException(ResultCode.CONNECT_ERROR, e.getMessage());
        }
    }

    private LDAPConnection createConnection() throws Exception {
        String url = config.getServerUrl();
        boolean isLdaps = url.startsWith("ldaps://");

        String host;
        int port;

        String hostPart = url.replaceFirst("ldaps?://", "");
        if (hostPart.contains(":")) {
            String[] parts = hostPart.split(":");
            host = parts[0];
            port = Integer.parseInt(parts[1]);
        } else {
            host = hostPart;
            port = isLdaps ? 636 : 389;
        }

        LDAPConnection connection;
        if (isLdaps || config.isUseSsl()) {
            SSLUtil sslUtil = new SSLUtil(new TrustAllTrustManager()); // TODO: Use proper trust manager
            SSLSocketFactory socketFactory = sslUtil.createSSLSocketFactory();
            connection = new LDAPConnection(socketFactory, host, port);
        } else {
            connection = new LDAPConnection(host, port);
        }

        // Bind if credentials provided
        if (config.getBindDn() != null && !config.getBindDn().isEmpty()) {
            connection.bind(config.getBindDn(), config.getBindPassword());
        }

        return connection;
    }

    /**
     * Authenticate a user with username and password.
     *
     * @return true if authentication succeeded
     */
    public boolean authenticate(String username, String password) {
        if (!config.isEnabled() || connectionPool == null) {
            return false;
        }

        try {
            // Find the user DN
            String userDn = findUserDn(username);
            if (userDn == null) {
                log.debug("User not found: {}", username);
                return false;
            }

            // Try to bind as the user
            LDAPConnection userConnection = createConnection();
            try {
                BindResult result = userConnection.bind(userDn, password);
                if (result.getResultCode() == ResultCode.SUCCESS) {
                    log.info("LDAP authentication successful: {}", username);
                    return true;
                }
            } finally {
                userConnection.close();
            }

            log.debug("LDAP authentication failed for user: {}", username);
            return false;

        } catch (Exception e) {
            log.error("LDAP authentication error for user: {}", username, e);
            return false;
        }
    }

    /**
     * Find the DN for a username.
     */
    public String findUserDn(String username) throws LDAPSearchException {
        if (connectionPool == null) {
            return null;
        }

        String searchFilter = config.getUserSearchFilter().replace("{0}", Filter.encodeValue(username));
        String searchBase = config.getFullUserSearchBase();

        SearchResult result = connectionPool.search(
                searchBase,
                SearchScope.SUB,
                searchFilter,
                "dn"
        );

        if (result.getEntryCount() > 0) {
            return result.getSearchEntries().get(0).getDN();
        }

        return null;
    }

    /**
     * Get the groups for a user.
     */
    public Set<String> getUserGroups(String username) {
        Set<String> groups = new HashSet<>();

        if (!config.isEnabled() || connectionPool == null) {
            return groups;
        }

        try {
            String userDn = findUserDn(username);
            if (userDn == null) {
                return groups;
            }

            String searchFilter = config.getGroupSearchFilter().replace("{0}", Filter.encodeValue(userDn));
            String searchBase = config.getFullGroupSearchBase();

            SearchResult result = connectionPool.search(
                    searchBase,
                    SearchScope.SUB,
                    searchFilter,
                    config.getGroupNameAttribute()
            );

            for (SearchResultEntry entry : result.getSearchEntries()) {
                String groupName = entry.getAttributeValue(config.getGroupNameAttribute());
                if (groupName != null) {
                    groups.add(config.getGroupRolePrefix() + groupName.toUpperCase());
                }
            }

        } catch (Exception e) {
            log.error("Error getting groups for user: {}", username, e);
        }

        return groups;
    }

    /**
     * Get user attributes.
     */
    public Map<String, String> getUserAttributes(String username, String... attributes)
            throws LDAPSearchException {
        Map<String, String> result = new HashMap<>();

        if (connectionPool == null) {
            return result;
        }

        String searchFilter = config.getUserSearchFilter().replace("{0}", Filter.encodeValue(username));
        String searchBase = config.getFullUserSearchBase();

        SearchResult searchResult = connectionPool.search(
                searchBase,
                SearchScope.SUB,
                searchFilter,
                attributes
        );

        if (searchResult.getEntryCount() > 0) {
            SearchResultEntry entry = searchResult.getSearchEntries().get(0);
            for (String attr : attributes) {
                String value = entry.getAttributeValue(attr);
                if (value != null) {
                    result.put(attr, value);
                }
            }
        }

        return result;
    }

    /**
     * Test the LDAP connection.
     */
    public boolean testConnection() {
        if (!config.isEnabled()) {
            return false;
        }

        try {
            LDAPConnection connection = createConnection();
            RootDSE rootDSE = connection.getRootDSE();
            connection.close();
            return rootDSE != null;
        } catch (Exception e) {
            log.error("LDAP connection test failed", e);
            return false;
        }
    }

    @Override
    public void close() {
        if (connectionPool != null) {
            connectionPool.close();
            log.info("LDAP connection pool closed");
        }
    }

    public boolean isEnabled() {
        return config.isEnabled() && connectionPool != null;
    }
}
