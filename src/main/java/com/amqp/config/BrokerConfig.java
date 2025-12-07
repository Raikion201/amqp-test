package com.amqp.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Comprehensive configuration for the AMQP broker.
 * Supports configuration from properties files, environment variables, and programmatic settings.
 */
public class BrokerConfig {

    // Server configuration
    private String host = "0.0.0.0";
    private int amqpPort = 5672;
    private int managementPort = 15672;
    private boolean sslEnabled = false;
    private String sslCertPath = null;
    private String sslKeyPath = null;

    // Connection configuration
    private int maxConnections = 1000;
    private int channelMax = 2047;
    private int frameMax = 131072;
    private int heartbeatSeconds = 60;
    private int handshakeTimeout = 10000; // milliseconds

    // Resource limits
    private long memoryHighWatermark = 400 * 1024 * 1024L; // 400 MB
    private long memoryLowWatermark = 200 * 1024 * 1024L; // 200 MB
    private long diskFreeLimit = 50 * 1024 * 1024L; // 50 MB

    // Queue defaults
    private int defaultQueueMaxLength = 100000;
    private long defaultQueueMaxBytes = 100 * 1024 * 1024L; // 100 MB
    private long defaultMessageTTL = 0; // 0 = no TTL
    private int defaultQueuePriority = 0;

    // Persistence configuration
    private boolean persistenceEnabled = true;
    private String persistenceType = "postgresql"; // postgresql, memory
    private String databaseUrl = "jdbc:postgresql://localhost:5432/amqp";
    private String databaseUser = "amcp";
    private String databasePassword = "amcp";
    private int databasePoolSize = 10;

    // Clustering configuration
    private boolean clusterEnabled = false;
    private String clusterNodeName = "node1";
    private String clusterNodes = ""; // Comma-separated list
    private int clusterPort = 25672;

    // Plugin configuration
    private boolean shovelPluginEnabled = false;
    private boolean federationPluginEnabled = false;
    private boolean stompPluginEnabled = false;
    private boolean mqttPluginEnabled = false;
    private boolean webSocketPluginEnabled = false;

    // Management configuration
    private boolean managementApiEnabled = true;
    private String managementAuthUser = "admin";
    private String managementAuthPassword = "admin";

    // Logging configuration
    private String logLevel = "INFO";
    private String logFile = "logs/amqp.log";
    private boolean logToConsole = true;

    // Performance tuning
    private int workerThreads = Runtime.getRuntime().availableProcessors() * 2;
    private int publisherConfirmPoolSize = 10;
    private int consumerThreadPoolSize = 10;

    public BrokerConfig() {
        // Load from environment variables if available
        loadFromEnvironment();
    }

    /**
     * Load configuration from environment variables.
     */
    private void loadFromEnvironment() {
        Map<String, String> env = System.getenv();

        if (env.containsKey("AMQP_HOST")) {
            host = env.get("AMQP_HOST");
        }
        if (env.containsKey("AMQP_PORT")) {
            amqpPort = Integer.parseInt(env.get("AMQP_PORT"));
        }
        if (env.containsKey("AMQP_MANAGEMENT_PORT")) {
            managementPort = Integer.parseInt(env.get("AMQP_MANAGEMENT_PORT"));
        }
        if (env.containsKey("AMQP_SSL_ENABLED")) {
            sslEnabled = Boolean.parseBoolean(env.get("AMQP_SSL_ENABLED"));
        }
        if (env.containsKey("AMQP_SSL_CERT")) {
            sslCertPath = env.get("AMQP_SSL_CERT");
        }
        if (env.containsKey("AMQP_SSL_KEY")) {
            sslKeyPath = env.get("AMQP_SSL_KEY");
        }
        if (env.containsKey("AMQP_DB_URL")) {
            databaseUrl = env.get("AMQP_DB_URL");
        }
        if (env.containsKey("AMQP_DB_USER")) {
            databaseUser = env.get("AMQP_DB_USER");
        }
        if (env.containsKey("AMQP_DB_PASSWORD")) {
            databasePassword = env.get("AMQP_DB_PASSWORD");
        }
    }

    /**
     * Load configuration from Properties object.
     */
    public void loadFromProperties(Properties properties) {
        if (properties.containsKey("host")) {
            host = properties.getProperty("host");
        }
        if (properties.containsKey("amqp.port")) {
            amqpPort = Integer.parseInt(properties.getProperty("amqp.port"));
        }
        if (properties.containsKey("management.port")) {
            managementPort = Integer.parseInt(properties.getProperty("management.port"));
        }
        if (properties.containsKey("ssl.enabled")) {
            sslEnabled = Boolean.parseBoolean(properties.getProperty("ssl.enabled"));
        }
        // Add more property mappings as needed
    }

    /**
     * Export configuration as a map.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("host", host);
        map.put("amqpPort", amqpPort);
        map.put("managementPort", managementPort);
        map.put("sslEnabled", sslEnabled);
        map.put("maxConnections", maxConnections);
        map.put("channelMax", channelMax);
        map.put("frameMax", frameMax);
        map.put("heartbeatSeconds", heartbeatSeconds);
        map.put("persistenceEnabled", persistenceEnabled);
        map.put("persistenceType", persistenceType);
        map.put("clusterEnabled", clusterEnabled);
        map.put("managementApiEnabled", managementApiEnabled);
        map.put("workerThreads", workerThreads);
        return map;
    }

    // Getters and setters

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getAmqpPort() {
        return amqpPort;
    }

    public void setAmqpPort(int amqpPort) {
        this.amqpPort = amqpPort;
    }

    public int getManagementPort() {
        return managementPort;
    }

    public void setManagementPort(int managementPort) {
        this.managementPort = managementPort;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public String getSslCertPath() {
        return sslCertPath;
    }

    public void setSslCertPath(String sslCertPath) {
        this.sslCertPath = sslCertPath;
    }

    public String getSslKeyPath() {
        return sslKeyPath;
    }

    public void setSslKeyPath(String sslKeyPath) {
        this.sslKeyPath = sslKeyPath;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public int getChannelMax() {
        return channelMax;
    }

    public void setChannelMax(int channelMax) {
        this.channelMax = channelMax;
    }

    public int getFrameMax() {
        return frameMax;
    }

    public void setFrameMax(int frameMax) {
        this.frameMax = frameMax;
    }

    public int getHeartbeatSeconds() {
        return heartbeatSeconds;
    }

    public void setHeartbeatSeconds(int heartbeatSeconds) {
        this.heartbeatSeconds = heartbeatSeconds;
    }

    public long getMemoryHighWatermark() {
        return memoryHighWatermark;
    }

    public void setMemoryHighWatermark(long memoryHighWatermark) {
        this.memoryHighWatermark = memoryHighWatermark;
    }

    public long getMemoryLowWatermark() {
        return memoryLowWatermark;
    }

    public void setMemoryLowWatermark(long memoryLowWatermark) {
        this.memoryLowWatermark = memoryLowWatermark;
    }

    public long getDiskFreeLimit() {
        return diskFreeLimit;
    }

    public void setDiskFreeLimit(long diskFreeLimit) {
        this.diskFreeLimit = diskFreeLimit;
    }

    public boolean isPersistenceEnabled() {
        return persistenceEnabled;
    }

    public void setPersistenceEnabled(boolean persistenceEnabled) {
        this.persistenceEnabled = persistenceEnabled;
    }

    public String getPersistenceType() {
        return persistenceType;
    }

    public void setPersistenceType(String persistenceType) {
        this.persistenceType = persistenceType;
    }

    public String getDatabaseUrl() {
        return databaseUrl;
    }

    public void setDatabaseUrl(String databaseUrl) {
        this.databaseUrl = databaseUrl;
    }

    public String getDatabaseUser() {
        return databaseUser;
    }

    public void setDatabaseUser(String databaseUser) {
        this.databaseUser = databaseUser;
    }

    public String getDatabasePassword() {
        return databasePassword;
    }

    public void setDatabasePassword(String databasePassword) {
        this.databasePassword = databasePassword;
    }

    public int getDatabasePoolSize() {
        return databasePoolSize;
    }

    public void setDatabasePoolSize(int databasePoolSize) {
        this.databasePoolSize = databasePoolSize;
    }

    public boolean isClusterEnabled() {
        return clusterEnabled;
    }

    public void setClusterEnabled(boolean clusterEnabled) {
        this.clusterEnabled = clusterEnabled;
    }

    public boolean isManagementApiEnabled() {
        return managementApiEnabled;
    }

    public void setManagementApiEnabled(boolean managementApiEnabled) {
        this.managementApiEnabled = managementApiEnabled;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    @Override
    public String toString() {
        return String.format("BrokerConfig{host='%s', amqpPort=%d, managementPort=%d, ssl=%s, persistence=%s}",
                           host, amqpPort, managementPort, sslEnabled, persistenceEnabled);
    }
}
