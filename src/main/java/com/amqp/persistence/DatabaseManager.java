package com.amqp.persistence;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseManager {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseManager.class);
    
    private final HikariDataSource dataSource;
    
    public DatabaseManager(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(20);
        config.setMinimumIdle(5);
        config.setIdleTimeout(300000);
        config.setConnectionTimeout(20000);
        config.setLeakDetectionThreshold(60000);
        
        this.dataSource = new HikariDataSource(config);
        
        initializeSchema();
        logger.info("Database manager initialized");
    }
    
    public DataSource getDataSource() {
        return dataSource;
    }
    
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
    
    private void initializeSchema() {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS exchanges (
                    vhost VARCHAR(255) NOT NULL DEFAULT '/',
                    name VARCHAR(255) NOT NULL,
                    type VARCHAR(50) NOT NULL,
                    durable BOOLEAN NOT NULL,
                    auto_delete BOOLEAN NOT NULL,
                    internal BOOLEAN NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (vhost, name)
                )
                """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS queues (
                    vhost VARCHAR(255) NOT NULL DEFAULT '/',
                    name VARCHAR(255) NOT NULL,
                    durable BOOLEAN NOT NULL,
                    exclusive BOOLEAN NOT NULL,
                    auto_delete BOOLEAN NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (vhost, name)
                )
                """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS bindings (
                    id SERIAL PRIMARY KEY,
                    vhost VARCHAR(255) NOT NULL DEFAULT '/',
                    exchange_name VARCHAR(255) NOT NULL,
                    queue_name VARCHAR(255) NOT NULL,
                    routing_key VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (vhost, exchange_name) REFERENCES exchanges(vhost, name) ON DELETE CASCADE,
                    FOREIGN KEY (vhost, queue_name) REFERENCES queues(vhost, name) ON DELETE CASCADE,
                    UNIQUE(vhost, exchange_name, queue_name, routing_key)
                )
                """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id BIGSERIAL PRIMARY KEY,
                    vhost VARCHAR(255) NOT NULL DEFAULT '/',
                    queue_name VARCHAR(255) NOT NULL,
                    routing_key VARCHAR(255),
                    content_type VARCHAR(100),
                    content_encoding VARCHAR(100),
                    headers TEXT,
                    delivery_mode SMALLINT,
                    priority SMALLINT,
                    correlation_id VARCHAR(255),
                    reply_to VARCHAR(255),
                    expiration VARCHAR(255),
                    message_id VARCHAR(255),
                    timestamp BIGINT,
                    type VARCHAR(255),
                    user_id VARCHAR(255),
                    app_id VARCHAR(255),
                    cluster_id VARCHAR(255),
                    body BYTEA,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (vhost, queue_name) REFERENCES queues(vhost, name) ON DELETE CASCADE
                )
                """);
            
            // Composite indexes including vhost for multi-tenant queries
            stmt.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_vhost_queue ON messages(vhost, queue_name)
                """);

            stmt.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at)
                """);

            stmt.execute("""
                CREATE INDEX IF NOT EXISTS idx_bindings_vhost_exchange ON bindings(vhost, exchange_name)
                """);

            stmt.execute("""
                CREATE INDEX IF NOT EXISTS idx_bindings_vhost_queue ON bindings(vhost, queue_name)
                """);

            // Indexes for exchange and queue lookups by vhost
            stmt.execute("""
                CREATE INDEX IF NOT EXISTS idx_exchanges_vhost ON exchanges(vhost)
                """);

            stmt.execute("""
                CREATE INDEX IF NOT EXISTS idx_queues_vhost ON queues(vhost)
                """);

            // Index for message expiration queries (standard index, compatible with H2 and PostgreSQL)
            stmt.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_expiration ON messages(expiration)
                """);

            logger.info("Database schema initialized with vhost-aware indexes");
            
        } catch (SQLException e) {
            logger.error("Failed to initialize database schema", e);
            throw new RuntimeException("Database initialization failed", e);
        }
    }
    
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            logger.info("Database connection pool closed");
        }
    }
}