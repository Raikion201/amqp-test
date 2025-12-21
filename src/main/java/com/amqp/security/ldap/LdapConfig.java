package com.amqp.security.ldap;

/**
 * LDAP authentication configuration.
 */
public class LdapConfig {

    private boolean enabled = false;

    // Connection settings
    private String serverUrl = "ldap://localhost:389";
    private boolean useSsl = false;
    private String bindDn;
    private String bindPassword;

    // Search settings
    private String baseDn;
    private String userSearchFilter = "(uid={0})";
    private String userSearchBase = "";
    private String usernameAttribute = "uid";

    // Group settings
    private String groupSearchBase = "";
    private String groupSearchFilter = "(member={0})";
    private String groupNameAttribute = "cn";
    private String groupRolePrefix = "ROLE_";

    // Connection pool
    private int poolSize = 10;
    private long connectionTimeout = 5000;
    private long responseTimeout = 10000;

    public LdapConfig() {
    }

    // Builder
    public static Builder builder() {
        return new Builder();
    }

    // Getters
    public boolean isEnabled() {
        return enabled;
    }

    public String getServerUrl() {
        return serverUrl;
    }

    public boolean isUseSsl() {
        return useSsl;
    }

    public String getBindDn() {
        return bindDn;
    }

    public String getBindPassword() {
        return bindPassword;
    }

    public String getBaseDn() {
        return baseDn;
    }

    public String getUserSearchFilter() {
        return userSearchFilter;
    }

    public String getUserSearchBase() {
        return userSearchBase;
    }

    public String getUsernameAttribute() {
        return usernameAttribute;
    }

    public String getGroupSearchBase() {
        return groupSearchBase;
    }

    public String getGroupSearchFilter() {
        return groupSearchFilter;
    }

    public String getGroupNameAttribute() {
        return groupNameAttribute;
    }

    public String getGroupRolePrefix() {
        return groupRolePrefix;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public long getConnectionTimeout() {
        return connectionTimeout;
    }

    public long getResponseTimeout() {
        return responseTimeout;
    }

    // Setters
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public void setUseSsl(boolean useSsl) {
        this.useSsl = useSsl;
    }

    public void setBindDn(String bindDn) {
        this.bindDn = bindDn;
    }

    public void setBindPassword(String bindPassword) {
        this.bindPassword = bindPassword;
    }

    public void setBaseDn(String baseDn) {
        this.baseDn = baseDn;
    }

    public void setUserSearchFilter(String userSearchFilter) {
        this.userSearchFilter = userSearchFilter;
    }

    public void setUserSearchBase(String userSearchBase) {
        this.userSearchBase = userSearchBase;
    }

    public void setUsernameAttribute(String usernameAttribute) {
        this.usernameAttribute = usernameAttribute;
    }

    public void setGroupSearchBase(String groupSearchBase) {
        this.groupSearchBase = groupSearchBase;
    }

    public void setGroupSearchFilter(String groupSearchFilter) {
        this.groupSearchFilter = groupSearchFilter;
    }

    public void setGroupNameAttribute(String groupNameAttribute) {
        this.groupNameAttribute = groupNameAttribute;
    }

    public void setGroupRolePrefix(String groupRolePrefix) {
        this.groupRolePrefix = groupRolePrefix;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public void setConnectionTimeout(long connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public void setResponseTimeout(long responseTimeout) {
        this.responseTimeout = responseTimeout;
    }

    /**
     * Get the full user search base DN.
     */
    public String getFullUserSearchBase() {
        if (userSearchBase == null || userSearchBase.isEmpty()) {
            return baseDn;
        }
        return userSearchBase + "," + baseDn;
    }

    /**
     * Get the full group search base DN.
     */
    public String getFullGroupSearchBase() {
        if (groupSearchBase == null || groupSearchBase.isEmpty()) {
            return baseDn;
        }
        return groupSearchBase + "," + baseDn;
    }

    public static class Builder {
        private final LdapConfig config = new LdapConfig();

        public Builder enabled(boolean enabled) {
            config.enabled = enabled;
            return this;
        }

        public Builder serverUrl(String serverUrl) {
            config.serverUrl = serverUrl;
            return this;
        }

        public Builder useSsl(boolean useSsl) {
            config.useSsl = useSsl;
            return this;
        }

        public Builder bindCredentials(String bindDn, String password) {
            config.bindDn = bindDn;
            config.bindPassword = password;
            return this;
        }

        public Builder baseDn(String baseDn) {
            config.baseDn = baseDn;
            return this;
        }

        public Builder userSearch(String base, String filter) {
            config.userSearchBase = base;
            config.userSearchFilter = filter;
            return this;
        }

        public Builder usernameAttribute(String attr) {
            config.usernameAttribute = attr;
            return this;
        }

        public Builder groupSearch(String base, String filter) {
            config.groupSearchBase = base;
            config.groupSearchFilter = filter;
            return this;
        }

        public Builder poolSize(int size) {
            config.poolSize = size;
            return this;
        }

        public Builder timeout(long connectionMs, long responseMs) {
            config.connectionTimeout = connectionMs;
            config.responseTimeout = responseMs;
            return this;
        }

        public LdapConfig build() {
            return config;
        }
    }
}
