package com.amqp.compliance;

import com.amqp.persistence.DatabaseManager;
import com.amqp.persistence.PersistenceManager;
import com.amqp.server.AmqpBroker;
import com.amqp.server.AmqpServer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test Harness for AMQP Spec Compliance Testing.
 *
 * This base class starts the AMQP server and provides RabbitMQ client
 * connections for testing spec compliance with real AMQP clients.
 *
 * Usage:
 * - Extend this class for integration tests
 * - Use getConnection() to get a real AMQP client connection
 * - Use createChannel() to create channels for testing
 *
 * Run tests with: mvn verify -DskipTests=false
 */
public abstract class AmqpServerTestHarness {

    protected static final Logger logger = LoggerFactory.getLogger(AmqpServerTestHarness.class);

    // Server configuration
    protected static final String HOST = "localhost";
    protected static final int PORT = 55672; // Use non-standard port to avoid conflicts
    protected static final String VHOST = "/";
    protected static final String USERNAME = "admin";
    protected static final String PASSWORD = "admin";

    // Server components
    protected static AmqpServer server;
    protected static AmqpBroker broker;
    protected static PersistenceManager persistenceManager;
    protected static Thread serverThread;
    protected static AtomicBoolean serverRunning = new AtomicBoolean(false);

    // Client connection factory
    protected static ConnectionFactory connectionFactory;

    // Per-test connection and channel
    protected Connection connection;
    protected Channel channel;

    /**
     * Start the AMQP server before all tests.
     */
    @BeforeAll
    static void startServer() throws Exception {
        logger.info("=== Starting AMQP Server Test Harness ===");

        // Create in-memory persistence for testing
        DatabaseManager dbManager = createTestDatabaseManager();
        persistenceManager = new PersistenceManager(dbManager);

        // Create broker with guest user enabled for testing
        broker = new AmqpBroker(persistenceManager, true);

        // Create test admin user with administrator tag
        Set<String> adminTags = new HashSet<>();
        adminTags.add("administrator");
        broker.getAuthenticationManager().createUser(USERNAME, PASSWORD, adminTags);
        broker.getAuthenticationManager().setUserPermissions(USERNAME, VHOST, ".*", ".*", ".*");

        // Create the server
        server = new AmqpServer(PORT, broker);

        // Start server in a separate thread (start() blocks)
        CountDownLatch serverStarted = new CountDownLatch(1);
        serverThread = new Thread(() -> {
            try {
                serverRunning.set(true);
                serverStarted.countDown();
                server.start();
            } catch (InterruptedException e) {
                logger.info("Server thread interrupted");
            } catch (Exception e) {
                logger.error("Server error", e);
            } finally {
                serverRunning.set(false);
            }
        }, "amqp-test-server");
        serverThread.setDaemon(true);
        serverThread.start();

        // Wait for server to start
        serverStarted.await(5, TimeUnit.SECONDS);

        // Wait for server to be accepting connections
        waitForServerReady(HOST, PORT, 10);

        // Configure RabbitMQ client factory
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(HOST);
        connectionFactory.setPort(PORT);
        connectionFactory.setVirtualHost(VHOST);
        connectionFactory.setUsername(USERNAME);
        connectionFactory.setPassword(PASSWORD);
        connectionFactory.setConnectionTimeout(10000);
        connectionFactory.setHandshakeTimeout(10000);
        connectionFactory.setRequestedHeartbeat(60);

        logger.info("AMQP Server started on {}:{}", HOST, PORT);
    }

    /**
     * Wait for the server to be ready to accept connections.
     */
    private static void waitForServerReady(String host, int port, int maxRetries) throws Exception {
        for (int i = 0; i < maxRetries; i++) {
            try (Socket socket = new Socket(host, port)) {
                logger.info("Server is ready on {}:{}", host, port);
                return;
            } catch (IOException e) {
                logger.debug("Waiting for server... attempt {}/{}", i + 1, maxRetries);
                Thread.sleep(500);
            }
        }
        throw new RuntimeException("Server failed to start within timeout");
    }
    
    /**
     * Stop the AMQP server after all tests.
     */
    @AfterAll
    static void stopServer() {
        logger.info("=== Stopping AMQP Server Test Harness ===");

        if (server != null) {
            server.stop();
        }
        if (broker != null) {
            broker.stop();
        }
        if (serverThread != null) {
            serverThread.interrupt();
            try {
                serverThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        logger.info("AMQP Server stopped");
    }
    
    /**
     * Create a new connection before each test.
     */
    @BeforeEach
    void setUp() throws IOException, TimeoutException {
        connection = connectionFactory.newConnection("test-connection");
        channel = connection.createChannel();
        logger.debug("Created test connection and channel");
    }
    
    /**
     * Close connection after each test.
     */
    @AfterEach
    void tearDown() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing test connection", e);
        }
    }
    
    /**
     * Get the current test connection.
     */
    protected Connection getConnection() {
        return connection;
    }
    
    /**
     * Create a new channel on the test connection.
     */
    protected Channel createChannel() throws IOException {
        return connection.createChannel();
    }
    
    /**
     * Create a new independent connection.
     */
    protected Connection createNewConnection() throws IOException, TimeoutException {
        return connectionFactory.newConnection();
    }
    
    /**
     * Create an in-memory database manager for testing.
     */
    private static DatabaseManager createTestDatabaseManager() {
        // Return a mock/in-memory database manager for testing
        return new TestDatabaseManager();
    }
    
    /**
     * In-memory database manager for testing (no real DB needed).
     * Uses H2 in-memory database.
     */
    private static class TestDatabaseManager extends DatabaseManager {
        public TestDatabaseManager() {
            super("jdbc:h2:mem:test_" + System.currentTimeMillis() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
                  "sa", "");
        }
    }
}
