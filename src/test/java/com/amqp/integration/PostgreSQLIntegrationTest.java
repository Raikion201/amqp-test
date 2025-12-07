package com.amqp.integration;

import com.amqp.AbstractPostgresTest;
import com.amqp.model.Exchange;
import com.amqp.model.Message;
import com.amqp.model.Queue;
import com.amqp.persistence.PersistenceManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

@DisplayName("PostgreSQL Integration Tests - Requires Docker")
@EnabledIfSystemProperty(named = "postgresql.tests.enabled", matches = "true", disabledReason = "PostgreSQL integration tests disabled. Enable with -Dpostgresql.tests.enabled=true and ensure Docker is running")
class PostgreSQLIntegrationTest extends AbstractPostgresTest {

    private PersistenceManager persistenceManager;

    @BeforeEach
    void setUp() throws SQLException {
        persistenceManager = new PersistenceManager(databaseManager);

        // Clean up test data before each test
        try (Connection conn = databaseManager.getConnection()) {
            conn.createStatement().execute("DELETE FROM messages");
            conn.createStatement().execute("DELETE FROM bindings");
            conn.createStatement().execute("DELETE FROM queues");
            conn.createStatement().execute("DELETE FROM exchanges");
        }
    }

    @Test
    @DisplayName("Should connect to PostgreSQL database successfully")
    void testDatabaseConnection() throws SQLException {
        try (Connection conn = databaseManager.getConnection()) {
            assertThat(conn).isNotNull();
            assertThat(conn.isClosed()).isFalse();

            // Verify we can query the database
            ResultSet rs = conn.createStatement().executeQuery("SELECT 1");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("Should verify schema is initialized correctly")
    void testSchemaInitialization() throws SQLException {
        try (Connection conn = databaseManager.getConnection()) {
            // Check if all required tables exist
            String[] tables = {"exchanges", "queues", "bindings", "messages"};

            for (String table : tables) {
                ResultSet rs = conn.getMetaData().getTables(null, null, table, new String[]{"TABLE"});
                assertThat(rs.next()).as("Table %s should exist", table).isTrue();
            }
        }
    }

    @Test
    @DisplayName("Should save and retrieve exchange from PostgreSQL")
    void testSaveAndRetrieveExchange() throws SQLException {
        Exchange exchange = new Exchange("test-exchange", Exchange.Type.DIRECT, true, false, false);

        persistenceManager.saveExchange(exchange);

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM exchanges WHERE name = ?")) {
            stmt.setString(1, "test-exchange");
            ResultSet rs = stmt.executeQuery();

            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("name")).isEqualTo("test-exchange");
            assertThat(rs.getString("type")).isEqualTo("DIRECT");
            assertThat(rs.getBoolean("durable")).isTrue();
            assertThat(rs.getBoolean("auto_delete")).isFalse();
            assertThat(rs.getBoolean("internal")).isFalse();
        }
    }

    @Test
    @DisplayName("Should save and retrieve queue from PostgreSQL")
    void testSaveAndRetrieveQueue() throws SQLException {
        Queue queue = new Queue("test-queue", true, false, false);

        persistenceManager.saveQueue(queue);

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM queues WHERE name = ?")) {
            stmt.setString(1, "test-queue");
            ResultSet rs = stmt.executeQuery();

            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("name")).isEqualTo("test-queue");
            assertThat(rs.getBoolean("durable")).isTrue();
            assertThat(rs.getBoolean("exclusive")).isFalse();
            assertThat(rs.getBoolean("auto_delete")).isFalse();
        }
    }

    @Test
    @DisplayName("Should save binding with foreign key constraints")
    void testSaveBindingWithConstraints() throws SQLException {
        // First create exchange and queue
        Exchange exchange = new Exchange("exchange1", Exchange.Type.TOPIC, true, false, false);
        Queue queue = new Queue("queue1", true, false, false);

        persistenceManager.saveExchange(exchange);
        persistenceManager.saveQueue(queue);
        persistenceManager.saveBinding("exchange1", "queue1", "test.*.key");

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM bindings WHERE exchange_name = ?")) {
            stmt.setString(1, "exchange1");
            ResultSet rs = stmt.executeQuery();

            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("exchange_name")).isEqualTo("exchange1");
            assertThat(rs.getString("queue_name")).isEqualTo("queue1");
            assertThat(rs.getString("routing_key")).isEqualTo("test.*.key");
        }
    }

    @Test
    @DisplayName("Should save persistent message to PostgreSQL")
    void testSavePersistentMessage() throws SQLException {
        // Create queue first
        Queue queue = new Queue("msg-queue", true, false, false);
        persistenceManager.saveQueue(queue);

        Message message = new Message("Hello PostgreSQL".getBytes());
        message.setDeliveryMode((short) 2);
        message.setRoutingKey("test.key");
        message.setContentType("text/plain");
        message.setCorrelationId("correlation-123");
        message.setMessageId("msg-123");
        message.setPriority((short) 5);

        persistenceManager.saveMessage("msg-queue", message);

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM messages WHERE queue_name = ?")) {
            stmt.setString(1, "msg-queue");
            ResultSet rs = stmt.executeQuery();

            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("queue_name")).isEqualTo("msg-queue");
            assertThat(rs.getString("routing_key")).isEqualTo("test.key");
            assertThat(rs.getString("content_type")).isEqualTo("text/plain");
            assertThat(rs.getString("correlation_id")).isEqualTo("correlation-123");
            assertThat(rs.getString("message_id")).isEqualTo("msg-123");
            assertThat(rs.getShort("priority")).isEqualTo((short) 5);
            assertThat(rs.getBytes("body")).isEqualTo("Hello PostgreSQL".getBytes());
        }
    }

    @Test
    @DisplayName("Should delete message from PostgreSQL")
    void testDeleteMessage() throws SQLException {
        Queue queue = new Queue("del-queue", true, false, false);
        persistenceManager.saveQueue(queue);

        Message message = new Message("Delete me".getBytes());
        message.setDeliveryMode((short) 2);
        persistenceManager.saveMessage("del-queue", message);

        // Get the message ID
        long messageId;
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT id FROM messages WHERE queue_name = ?")) {
            stmt.setString(1, "del-queue");
            ResultSet rs = stmt.executeQuery();
            assertThat(rs.next()).isTrue();
            messageId = rs.getLong("id");
        }

        persistenceManager.deleteMessage(messageId);

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM messages WHERE id = ?")) {
            stmt.setLong(1, messageId);
            ResultSet rs = stmt.executeQuery();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    @DisplayName("Should handle cascade delete when exchange is deleted")
    void testCascadeDeleteExchange() throws SQLException {
        Exchange exchange = new Exchange("cascade-exchange", Exchange.Type.FANOUT, true, false, false);
        Queue queue = new Queue("cascade-queue", true, false, false);

        persistenceManager.saveExchange(exchange);
        persistenceManager.saveQueue(queue);
        persistenceManager.saveBinding("cascade-exchange", "cascade-queue", "");

        persistenceManager.deleteExchange("cascade-exchange");

        // Verify binding is also deleted (cascade)
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM bindings WHERE exchange_name = ?")) {
            stmt.setString(1, "cascade-exchange");
            ResultSet rs = stmt.executeQuery();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    @DisplayName("Should handle cascade delete when queue is deleted")
    void testCascadeDeleteQueue() throws SQLException {
        Queue queue = new Queue("cascade-queue2", true, false, false);
        persistenceManager.saveQueue(queue);

        Message message = new Message("Cascade test".getBytes());
        message.setDeliveryMode((short) 2);
        persistenceManager.saveMessage("cascade-queue2", message);

        persistenceManager.deleteQueue("cascade-queue2");

        // Verify messages are also deleted (cascade)
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM messages WHERE queue_name = ?")) {
            stmt.setString(1, "cascade-queue2");
            ResultSet rs = stmt.executeQuery();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    @DisplayName("Should handle concurrent message inserts")
    void testConcurrentMessageInserts() throws InterruptedException {
        Queue queue = new Queue("concurrent-queue", true, false, false);
        persistenceManager.saveQueue(queue);

        int threadCount = 10;
        int messagesPerThread = 50;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < messagesPerThread; j++) {
                    Message message = new Message(
                        String.format("Thread %d - Message %d", threadId, j).getBytes()
                    );
                    message.setDeliveryMode((short) 2);
                    persistenceManager.saveMessage("concurrent-queue", message);
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // Verify all messages were inserted
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) FROM messages WHERE queue_name = ?")) {
            stmt.setString(1, "concurrent-queue");
            ResultSet rs = stmt.executeQuery();
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(threadCount * messagesPerThread);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("Should enforce unique constraint on bindings")
    void testUniqueConstraintOnBindings() throws SQLException {
        Exchange exchange = new Exchange("unique-exchange", Exchange.Type.DIRECT, true, false, false);
        Queue queue = new Queue("unique-queue", true, false, false);

        persistenceManager.saveExchange(exchange);
        persistenceManager.saveQueue(queue);
        persistenceManager.saveBinding("unique-exchange", "unique-queue", "test.key");

        // Try to insert duplicate binding
        assertThatThrownBy(() -> {
            try (Connection conn = databaseManager.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                     "INSERT INTO bindings (exchange_name, queue_name, routing_key) VALUES (?, ?, ?)")) {
                stmt.setString(1, "unique-exchange");
                stmt.setString(2, "unique-queue");
                stmt.setString(3, "test.key");
                stmt.executeUpdate();
            }
        }).isInstanceOf(SQLException.class);
    }

    @Test
    @DisplayName("Should load messages from PostgreSQL on broker restart")
    void testLoadMessagesOnRestart() throws SQLException {
        Queue queue = new Queue("restart-queue", true, false, false);
        persistenceManager.saveQueue(queue);

        // Save multiple messages
        for (int i = 0; i < 5; i++) {
            Message message = new Message(("Message " + i).getBytes());
            message.setDeliveryMode((short) 2);
            message.setRoutingKey("test.key." + i);
            persistenceManager.saveMessage("restart-queue", message);
        }

        // Simulate broker restart by creating new persistence manager
        PersistenceManager newPersistenceManager = new PersistenceManager(databaseManager);

        // Verify messages are still in database
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT * FROM messages WHERE queue_name = ? ORDER BY id")) {
            stmt.setString(1, "restart-queue");
            ResultSet rs = stmt.executeQuery();

            int count = 0;
            while (rs.next()) {
                assertThat(new String(rs.getBytes("body"))).isEqualTo("Message " + count);
                count++;
            }
            assertThat(count).isEqualTo(5);
        }
    }

    @Test
    @DisplayName("Should handle all exchange types")
    void testAllExchangeTypes() throws SQLException {
        Exchange.Type[] types = {
            Exchange.Type.DIRECT,
            Exchange.Type.FANOUT,
            Exchange.Type.TOPIC,
            Exchange.Type.HEADERS
        };

        for (Exchange.Type type : types) {
            Exchange exchange = new Exchange("exchange-" + type, type, true, false, false);
            persistenceManager.saveExchange(exchange);
        }

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) FROM exchanges")) {
            ResultSet rs = stmt.executeQuery();
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(4);
        }
    }

    @Test
    @DisplayName("Should verify indexes are created for performance")
    void testIndexesExist() throws SQLException {
        try (Connection conn = databaseManager.getConnection()) {
            ResultSet rs = conn.getMetaData().getIndexInfo(null, null, "messages", false, false);

            boolean foundQueueNameIndex = false;
            boolean foundCreatedAtIndex = false;

            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                if ("idx_messages_queue_name".equals(indexName)) {
                    foundQueueNameIndex = true;
                } else if ("idx_messages_created_at".equals(indexName)) {
                    foundCreatedAtIndex = true;
                }
            }

            assertThat(foundQueueNameIndex).as("Queue name index should exist").isTrue();
            assertThat(foundCreatedAtIndex).as("Created at index should exist").isTrue();
        }
    }
}
