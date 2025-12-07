package com.amqp.persistence;

import com.amqp.model.Exchange;
import com.amqp.model.Message;
import com.amqp.model.Queue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Persistence Manager Tests")
class PersistenceManagerTest {

    @Mock
    private DatabaseManager databaseManager;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    private PersistenceManager persistenceManager;

    @BeforeEach
    void setUp() throws SQLException {
        persistenceManager = new PersistenceManager(databaseManager);
        lenient().when(databaseManager.getConnection()).thenReturn(connection);
        lenient().when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        lenient().when(preparedStatement.executeUpdate()).thenReturn(1);
    }

    @Test
    @DisplayName("Should save durable exchange")
    void testSaveExchange() throws SQLException {
        Exchange exchange = new Exchange("test-exchange", Exchange.Type.DIRECT, true, false, false);

        persistenceManager.saveExchange(exchange);

        verify(preparedStatement).setString(1, "test-exchange");
        verify(preparedStatement).setString(2, "DIRECT");
    }

    @Test
    @DisplayName("Should save durable queue")
    void testSaveQueue() throws SQLException {
        Queue queue = new Queue("test-queue", true, false, false);

        persistenceManager.saveQueue(queue);

        verify(preparedStatement).setString(1, "test-queue");
    }

    @Test
    @DisplayName("Should save queue binding")
    void testSaveBinding() throws SQLException {
        persistenceManager.saveBinding("test-exchange", "test-queue", "test.key");

        verify(preparedStatement).setString(1, "test-exchange");
        verify(preparedStatement).setString(2, "test-queue");
        verify(preparedStatement).setString(3, "test.key");
    }

    @Test
    @DisplayName("Should save persistent message")
    void testSaveMessage() throws SQLException {
        Message message = new Message("Test message".getBytes());
        message.setDeliveryMode((short) 2);
        message.setRoutingKey("test.key");

        persistenceManager.saveMessage("test-queue", message);

        verify(preparedStatement).setString(1, "test-queue");
        verify(preparedStatement).setString(2, "test.key");
    }

    @Test
    @DisplayName("Should delete message after consumption")
    void testDeleteMessage() throws SQLException {
        long messageId = 12345L;

        persistenceManager.deleteMessage(messageId);

        verify(preparedStatement).setLong(1, messageId);
    }

    @Test
    @DisplayName("Should handle database connection errors gracefully")
    void testDatabaseErrorHandling() throws SQLException {
        when(databaseManager.getConnection()).thenThrow(new SQLException("Database connection failed"));

        Exchange exchange = new Exchange("test-exchange", Exchange.Type.DIRECT, true, false, false);

        // Should not throw exception, but log error
        assertThatCode(() -> persistenceManager.saveExchange(exchange))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should delete exchange")
    void testDeleteExchange() throws SQLException {
        persistenceManager.deleteExchange("test-exchange");

        verify(preparedStatement).setString(1, "test-exchange");
    }

    @Test
    @DisplayName("Should delete queue")
    void testDeleteQueue() throws SQLException {
        persistenceManager.deleteQueue("test-queue");

        verify(preparedStatement).setString(1, "test-queue");
    }

    @Test
    @DisplayName("Should delete binding")
    void testDeleteBinding() throws SQLException {
        persistenceManager.deleteBinding("test-exchange", "test-queue", "test.key");

        verify(preparedStatement).setString(1, "test-exchange");
        verify(preparedStatement).setString(2, "test-queue");
        verify(preparedStatement).setString(3, "test.key");
    }
}