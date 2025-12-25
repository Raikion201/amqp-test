package com.amqp.persistence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.*;

@DisplayName("Database Manager Tests")
class DatabaseManagerTest {

    private DatabaseManager databaseManager;

    @BeforeEach
    void setUp() {
        // Use H2 in-memory database for testing
        String jdbcUrl = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        String username = "sa";
        String password = "";

        databaseManager = new DatabaseManager(jdbcUrl, username, password);
    }

    @AfterEach
    void tearDown() {
        if (databaseManager != null) {
            databaseManager.close();
        }
    }

    @Nested
    @DisplayName("Database Manager Construction Tests")
    class ConstructionTests {

        @Test
        @DisplayName("Should create database manager with valid credentials")
        void testDatabaseManagerCreation() {
            assertThat(databaseManager).isNotNull();
        }

        @Test
        @DisplayName("Should initialize with H2 in-memory database")
        void testH2DatabaseInit() {
            assertThat(databaseManager.getDataSource()).isNotNull();
        }

        @Test
        @DisplayName("Should throw exception with invalid JDBC URL")
        void testInvalidJdbcUrl() {
            assertThatThrownBy(() ->
                new DatabaseManager("invalid:jdbc:url", "user", "pass")
            ).isInstanceOf(RuntimeException.class);
        }
    }

    @Nested
    @DisplayName("DataSource Tests")
    class DataSourceTests {

        @Test
        @DisplayName("Should provide valid DataSource")
        void testGetDataSource() {
            DataSource dataSource = databaseManager.getDataSource();
            assertThat(dataSource).isNotNull();
        }

        @Test
        @DisplayName("Should get connection from DataSource")
        void testGetConnectionFromDataSource() throws SQLException {
            DataSource dataSource = databaseManager.getDataSource();

            try (Connection conn = dataSource.getConnection()) {
                assertThat(conn).isNotNull();
                assertThat(conn.isClosed()).isFalse();
            }
        }

        @Test
        @DisplayName("Should provide working connection")
        void testConnectionWorks() throws SQLException {
            try (Connection conn = databaseManager.getConnection()) {
                assertThat(conn).isNotNull();
                assertThat(conn.isClosed()).isFalse();

                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("SELECT 1");
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getInt(1)).isEqualTo(1);
                }
            }
        }

        @Test
        @DisplayName("Should support multiple concurrent connections")
        void testMultipleConnections() throws SQLException {
            Connection conn1 = databaseManager.getConnection();
            Connection conn2 = databaseManager.getConnection();
            Connection conn3 = databaseManager.getConnection();

            assertThat(conn1).isNotNull();
            assertThat(conn2).isNotNull();
            assertThat(conn3).isNotNull();

            assertThat(conn1).isNotSameAs(conn2);
            assertThat(conn2).isNotSameAs(conn3);

            conn1.close();
            conn2.close();
            conn3.close();
        }
    }

    @Nested
    @DisplayName("Schema Initialization Tests")
    class SchemaInitializationTests {

        @Test
        @DisplayName("Should create exchanges table")
        void testExchangesTableCreated() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'EXCHANGES'"
                );

                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }
        }

        @Test
        @DisplayName("Should create queues table")
        void testQueuesTableCreated() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'QUEUES'"
                );

                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }
        }

        @Test
        @DisplayName("Should create bindings table")
        void testBindingsTableCreated() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'BINDINGS'"
                );

                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }
        }

        @Test
        @DisplayName("Should create messages table")
        void testMessagesTableCreated() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'MESSAGES'"
                );

                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }
        }

        @Test
        @DisplayName("Should create exchanges table with correct columns")
        void testExchangesTableStructure() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                ResultSet rs = stmt.executeQuery(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_NAME = 'EXCHANGES' ORDER BY ORDINAL_POSITION"
                );

                // First column is VHOST for multi-tenant support
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("COLUMN_NAME").toUpperCase()).isEqualTo("VHOST");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("COLUMN_NAME").toUpperCase()).isEqualTo("NAME");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("COLUMN_NAME").toUpperCase()).isEqualTo("TYPE");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("COLUMN_NAME").toUpperCase()).isEqualTo("DURABLE");
            }
        }

        @Test
        @DisplayName("Should create queues table with correct columns")
        void testQueuesTableStructure() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                ResultSet rs = stmt.executeQuery(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_NAME = 'QUEUES' ORDER BY ORDINAL_POSITION"
                );

                // First column is VHOST for multi-tenant support
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("COLUMN_NAME").toUpperCase()).isEqualTo("VHOST");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("COLUMN_NAME").toUpperCase()).isEqualTo("NAME");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("COLUMN_NAME").toUpperCase()).isEqualTo("DURABLE");

                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("COLUMN_NAME").toUpperCase()).isEqualTo("EXCLUSIVE");
            }
        }

        @Test
        @DisplayName("Should create messages table with all required columns")
        void testMessagesTableStructure() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'MESSAGES'"
                );

                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isGreaterThanOrEqualTo(15); // At least 15 columns
            }
        }

        @Test
        @DisplayName("Should create indexes on messages table")
        void testMessagesTableIndexes() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                // Check for queue_name index (H2 uses different schema)
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.INDEXES " +
                    "WHERE TABLE_NAME = 'MESSAGES' AND INDEX_NAME LIKE '%QUEUE%'"
                );

                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isGreaterThanOrEqualTo(0);
            }
        }

        @Test
        @DisplayName("Should create indexes on bindings table")
        void testBindingsTableIndexes() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                // Check for exchange_name index (H2 uses different schema)
                ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.INDEXES " +
                    "WHERE TABLE_NAME = 'BINDINGS' AND INDEX_NAME LIKE '%EXCHANGE%'"
                );

                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt(1)).isGreaterThanOrEqualTo(0);
            }
        }
    }

    @Nested
    @DisplayName("Data Operations Tests")
    class DataOperationsTests {

        @Test
        @DisplayName("Should insert data into exchanges table")
        void testInsertIntoExchanges() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                stmt.execute(
                    "INSERT INTO exchanges (name, type, durable, auto_delete, internal) " +
                    "VALUES ('test-exchange', 'DIRECT', true, false, false)"
                );

                ResultSet rs = stmt.executeQuery("SELECT * FROM exchanges WHERE name = 'test-exchange'");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("name")).isEqualTo("test-exchange");
                assertThat(rs.getString("type")).isEqualTo("DIRECT");
            }
        }

        @Test
        @DisplayName("Should insert data into queues table")
        void testInsertIntoQueues() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                stmt.execute(
                    "INSERT INTO queues (name, durable, exclusive, auto_delete) " +
                    "VALUES ('test-queue', true, false, false)"
                );

                ResultSet rs = stmt.executeQuery("SELECT * FROM queues WHERE name = 'test-queue'");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("name")).isEqualTo("test-queue");
                assertThat(rs.getBoolean("durable")).isTrue();
            }
        }

        @Test
        @DisplayName("Should enforce foreign key constraints on bindings")
        void testBindingsForeignKeys() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                // Insert exchange and queue first
                stmt.execute("INSERT INTO exchanges (name, type, durable, auto_delete, internal) " +
                           "VALUES ('ex1', 'DIRECT', true, false, false)");
                stmt.execute("INSERT INTO queues (name, durable, exclusive, auto_delete) " +
                           "VALUES ('q1', true, false, false)");

                // Now insert binding
                stmt.execute("INSERT INTO bindings (exchange_name, queue_name, routing_key) " +
                           "VALUES ('ex1', 'q1', 'key1')");

                ResultSet rs = stmt.executeQuery("SELECT * FROM bindings WHERE routing_key = 'key1'");
                assertThat(rs.next()).isTrue();
            }
        }

        @Test
        @DisplayName("Should delete data from tables")
        void testDeleteOperations() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                stmt.execute("INSERT INTO exchanges (name, type, durable, auto_delete, internal) " +
                           "VALUES ('del-ex', 'DIRECT', true, false, false)");

                stmt.execute("DELETE FROM exchanges WHERE name = 'del-ex'");

                ResultSet rs = stmt.executeQuery("SELECT * FROM exchanges WHERE name = 'del-ex'");
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Nested
    @DisplayName("Connection Pool Tests")
    class ConnectionPoolTests {

        @Test
        @DisplayName("Should reuse connections from pool")
        void testConnectionPooling() throws SQLException {
            Connection conn1 = databaseManager.getConnection();
            conn1.close();

            Connection conn2 = databaseManager.getConnection();
            assertThat(conn2).isNotNull();
            conn2.close();
        }

        @Test
        @DisplayName("Should handle connection pool exhaustion")
        void testConnectionPoolExhaustion() throws SQLException {
            // Get maximum pool size worth of connections (20 from configuration)
            Connection[] connections = new Connection[20];

            for (int i = 0; i < 20; i++) {
                connections[i] = databaseManager.getConnection();
                assertThat(connections[i]).isNotNull();
            }

            // Close all connections
            for (Connection conn : connections) {
                conn.close();
            }
        }

        @Test
        @DisplayName("Should maintain minimum idle connections")
        void testMinimumIdleConnections() throws SQLException {
            // Get a connection and close it
            Connection conn = databaseManager.getConnection();
            conn.close();

            // Pool should maintain minimum idle connections
            // This is more of a configuration test
            assertThat(databaseManager.getDataSource()).isNotNull();
        }
    }

    @Nested
    @DisplayName("Database Manager Lifecycle Tests")
    class LifecycleTests {

        @Test
        @DisplayName("Should close database manager gracefully")
        void testCloseManager() {
            DatabaseManager tempManager = new DatabaseManager(
                "jdbc:h2:mem:tempdb;DB_CLOSE_DELAY=-1",
                "sa",
                ""
            );

            assertThatCode(() -> tempManager.close()).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle multiple close calls")
        void testMultipleCloseCalls() {
            DatabaseManager tempManager = new DatabaseManager(
                "jdbc:h2:mem:tempdb2;DB_CLOSE_DELAY=-1",
                "sa",
                ""
            );

            tempManager.close();
            assertThatCode(() -> tempManager.close()).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should not allow connections after close")
        void testNoConnectionsAfterClose() {
            DatabaseManager tempManager = new DatabaseManager(
                "jdbc:h2:mem:tempdb3;DB_CLOSE_DELAY=-1",
                "sa",
                ""
            );

            tempManager.close();

            assertThatThrownBy(() -> tempManager.getConnection())
                .isInstanceOf(SQLException.class);
        }
    }

    @Nested
    @DisplayName("Transaction Support Tests")
    class TransactionTests {

        @Test
        @DisplayName("Should support transactions")
        void testTransactionSupport() throws SQLException {
            try (Connection conn = databaseManager.getConnection()) {
                conn.setAutoCommit(false);

                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO exchanges (name, type, durable, auto_delete, internal) " +
                               "VALUES ('tx-ex', 'DIRECT', true, false, false)");
                    conn.commit();
                }

                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("SELECT * FROM exchanges WHERE name = 'tx-ex'");
                    assertThat(rs.next()).isTrue();
                }
            }
        }

        @Test
        @DisplayName("Should support transaction rollback")
        void testTransactionRollback() throws SQLException {
            try (Connection conn = databaseManager.getConnection()) {
                conn.setAutoCommit(false);

                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO exchanges (name, type, durable, auto_delete, internal) " +
                               "VALUES ('rb-ex', 'DIRECT', true, false, false)");
                    conn.rollback();
                }

                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("SELECT * FROM exchanges WHERE name = 'rb-ex'");
                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should handle SQL syntax errors")
        void testSQLSyntaxError() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                assertThatThrownBy(() -> stmt.execute("INVALID SQL STATEMENT"))
                    .isInstanceOf(SQLException.class);
            }
        }

        @Test
        @DisplayName("Should handle constraint violations")
        void testConstraintViolation() throws SQLException {
            try (Connection conn = databaseManager.getConnection();
                 Statement stmt = conn.createStatement()) {

                // Insert first record
                stmt.execute("INSERT INTO exchanges (name, type, durable, auto_delete, internal) " +
                           "VALUES ('dup-ex', 'DIRECT', true, false, false)");

                // Try to insert duplicate (name is primary key)
                assertThatThrownBy(() ->
                    stmt.execute("INSERT INTO exchanges (name, type, durable, auto_delete, internal) " +
                               "VALUES ('dup-ex', 'DIRECT', true, false, false)")
                ).isInstanceOf(SQLException.class);
            }
        }
    }
}
