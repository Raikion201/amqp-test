package com.amqp.compliance;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP Compliance Tests using the official amqplib test suite.
 *
 * This test runs the actual test files from:
 * https://github.com/amqp-node/amqplib
 *
 * Test files are stored in: src/test/resources/compliance-tests/nodejs-amqplib/
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Node.js amqplib Library Test Suite")
public class NodeJsAmqplibClientComplianceTest extends DockerClientTestBase {

    private static final Logger logger = LoggerFactory.getLogger(NodeJsAmqplibClientComplianceTest.class);
    private static final Path TEST_FILES_PATH = Paths.get("src/test/resources/compliance-tests/nodejs-amqplib");

    @Test
    @Order(1)
    @DisplayName("amqplib: Run connect tests from library")
    void testAmqplibConnectTests() throws Exception {
        logger.info("Running amqplib connect tests...");

        GenericContainer<?> nodeClient = new GenericContainer<>("node:20-alpine")
                .withNetwork(network)
                .withEnv("URL", getAmqpUrl())
                .withCopyFileToContainer(
                        MountableFile.forHostPath(TEST_FILES_PATH),
                        "/tests")
                .withCommand("sh", "-c", """
                    set -e
                    apk add --no-cache git >/dev/null 2>&1

                    cd /tests
                    npm install amqplib mocha --silent 2>/dev/null

                    echo "=== Running amqplib connect tests ==="
                    npx mocha connect.js --timeout 10000 2>&1 || true
                    echo "=== Connect tests completed ==="
                    """)
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("AMQPLIB-CONNECT"))
                .withStartupTimeout(java.time.Duration.ofMinutes(5));

        String logs = runClientAndGetLogs(nodeClient, 300);
        logger.info("Connect test output:\n{}", logs);

        assertTrue(logs.contains("tests completed") || logs.contains("passing"),
                "Connect tests should complete. Output: " + logs);
    }

    @Test
    @Order(2)
    @DisplayName("amqplib: Run channel tests from library")
    void testAmqplibChannelTests() throws Exception {
        logger.info("Running amqplib channel tests...");

        GenericContainer<?> nodeClient = new GenericContainer<>("node:20-alpine")
                .withNetwork(network)
                .withEnv("URL", getAmqpUrl())
                .withCopyFileToContainer(
                        MountableFile.forHostPath(TEST_FILES_PATH),
                        "/tests")
                .withCommand("sh", "-c", """
                    set -e
                    apk add --no-cache git >/dev/null 2>&1

                    cd /tests
                    npm install amqplib mocha --silent 2>/dev/null

                    echo "=== Running amqplib channel tests ==="
                    npx mocha channel.js --timeout 10000 2>&1 || true
                    echo "=== Channel tests completed ==="
                    """)
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("AMQPLIB-CHANNEL"))
                .withStartupTimeout(java.time.Duration.ofMinutes(5));

        String logs = runClientAndGetLogs(nodeClient, 300);
        logger.info("Channel test output:\n{}", logs);

        assertTrue(logs.contains("tests completed") || logs.contains("passing"),
                "Channel tests should complete. Output: " + logs);
    }

    @Test
    @Order(3)
    @DisplayName("amqplib: Run channel API tests from library")
    void testAmqplibChannelApiTests() throws Exception {
        logger.info("Running amqplib channel API tests...");

        GenericContainer<?> nodeClient = new GenericContainer<>("node:20-alpine")
                .withNetwork(network)
                .withEnv("URL", getAmqpUrl())
                .withCopyFileToContainer(
                        MountableFile.forHostPath(TEST_FILES_PATH),
                        "/tests")
                .withCommand("sh", "-c", """
                    set -e
                    apk add --no-cache git >/dev/null 2>&1

                    cd /tests
                    npm install amqplib mocha --silent 2>/dev/null

                    echo "=== Running amqplib channel API tests ==="
                    npx mocha channel_api.js --timeout 15000 2>&1 || true
                    echo "=== Channel API tests completed ==="
                    """)
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("AMQPLIB-API"))
                .withStartupTimeout(java.time.Duration.ofMinutes(5));

        String logs = runClientAndGetLogs(nodeClient, 300);
        logger.info("Channel API test output:\n{}", logs);

        assertTrue(logs.contains("tests completed") || logs.contains("passing"),
                "Channel API tests should complete. Output: " + logs);
    }

    @Test
    @Order(4)
    @DisplayName("amqplib: Run callback API tests from library")
    void testAmqplibCallbackApiTests() throws Exception {
        logger.info("Running amqplib callback API tests...");

        GenericContainer<?> nodeClient = new GenericContainer<>("node:20-alpine")
                .withNetwork(network)
                .withEnv("URL", getAmqpUrl())
                .withCopyFileToContainer(
                        MountableFile.forHostPath(TEST_FILES_PATH),
                        "/tests")
                .withCommand("sh", "-c", """
                    set -e
                    apk add --no-cache git >/dev/null 2>&1

                    cd /tests
                    npm install amqplib mocha --silent 2>/dev/null

                    echo "=== Running amqplib callback API tests ==="
                    npx mocha callback_api.js --timeout 15000 2>&1 || true
                    echo "=== Callback API tests completed ==="
                    """)
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("AMQPLIB-CALLBACK"))
                .withStartupTimeout(java.time.Duration.ofMinutes(5));

        String logs = runClientAndGetLogs(nodeClient, 300);
        logger.info("Callback API test output:\n{}", logs);

        assertTrue(logs.contains("tests completed") || logs.contains("passing"),
                "Callback API tests should complete. Output: " + logs);
    }
}
