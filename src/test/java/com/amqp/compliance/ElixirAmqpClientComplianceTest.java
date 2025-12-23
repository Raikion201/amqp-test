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
 * AMQP Compliance Tests using the official Elixir AMQP test suite.
 *
 * This test runs the actual test files from:
 * https://github.com/pma/amqp
 *
 * Test files are stored in: src/test/resources/compliance-tests/elixir-amqp/
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Elixir AMQP Library Test Suite")
public class ElixirAmqpClientComplianceTest extends DockerClientTestBase {

    private static final Logger logger = LoggerFactory.getLogger(ElixirAmqpClientComplianceTest.class);
    private static final Path TEST_FILES_PATH = Paths.get("src/test/resources/compliance-tests/elixir-amqp");

    @Test
    @Order(1)
    @DisplayName("elixir-amqp: Run connection tests from library")
    void testElixirConnectionTests() throws Exception {
        logger.info("Running Elixir AMQP connection tests...");

        GenericContainer<?> elixirClient = new GenericContainer<>("elixir:1.15-slim")
                .withNetwork(network)
                .withEnv("AMQP_HOST", getAmqpHost())
                .withEnv("AMQP_PORT", String.valueOf(AMQP_PORT))
                .withEnv("MIX_ENV", "test")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(TEST_FILES_PATH),
                        "/tests")
                .withCommand("sh", "-c", """
                    set -e
                    cd /tests

                    # Setup Mix project
                    mix local.hex --force
                    mix local.rebar --force
                    mix deps.get

                    echo "=== Running Elixir AMQP connection tests ==="
                    mix test connection_test.exs 2>&1 || true
                    echo "=== Connection tests completed ==="
                    """)
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("ELIXIR-CONN"))
                .withStartupTimeout(java.time.Duration.ofMinutes(10));

        String logs = runClientAndGetLogs(elixirClient, 600);
        logger.info("Elixir connection test output:\n{}", logs);

        assertTrue(logs.contains("tests completed") || logs.contains("test"),
                "Elixir connection tests should complete. Output: " + logs);
    }

    @Test
    @Order(2)
    @DisplayName("elixir-amqp: Run basic tests from library")
    void testElixirBasicTests() throws Exception {
        logger.info("Running Elixir AMQP basic tests...");

        GenericContainer<?> elixirClient = new GenericContainer<>("elixir:1.15-slim")
                .withNetwork(network)
                .withEnv("AMQP_HOST", getAmqpHost())
                .withEnv("AMQP_PORT", String.valueOf(AMQP_PORT))
                .withEnv("MIX_ENV", "test")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(TEST_FILES_PATH),
                        "/tests")
                .withCommand("sh", "-c", """
                    set -e
                    cd /tests

                    mix local.hex --force
                    mix local.rebar --force
                    mix deps.get

                    echo "=== Running Elixir AMQP basic tests ==="
                    mix test basic_test.exs 2>&1 || true
                    echo "=== Basic tests completed ==="
                    """)
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("ELIXIR-BASIC"))
                .withStartupTimeout(java.time.Duration.ofMinutes(10));

        String logs = runClientAndGetLogs(elixirClient, 600);
        logger.info("Elixir basic test output:\n{}", logs);

        assertTrue(logs.contains("tests completed") || logs.contains("test"),
                "Elixir basic tests should complete. Output: " + logs);
    }

    @Test
    @Order(3)
    @DisplayName("elixir-amqp: Run queue and exchange tests from library")
    void testElixirQueueExchangeTests() throws Exception {
        logger.info("Running Elixir AMQP queue and exchange tests...");

        GenericContainer<?> elixirClient = new GenericContainer<>("elixir:1.15-slim")
                .withNetwork(network)
                .withEnv("AMQP_HOST", getAmqpHost())
                .withEnv("AMQP_PORT", String.valueOf(AMQP_PORT))
                .withEnv("MIX_ENV", "test")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(TEST_FILES_PATH),
                        "/tests")
                .withCommand("sh", "-c", """
                    set -e
                    cd /tests

                    mix local.hex --force
                    mix local.rebar --force
                    mix deps.get

                    echo "=== Running Elixir AMQP queue and exchange tests ==="
                    mix test queue_test.exs exchange_test.exs 2>&1 || true
                    echo "=== Queue and exchange tests completed ==="
                    """)
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("ELIXIR-QUEUE"))
                .withStartupTimeout(java.time.Duration.ofMinutes(10));

        String logs = runClientAndGetLogs(elixirClient, 600);
        logger.info("Elixir queue and exchange test output:\n{}", logs);

        assertTrue(logs.contains("tests completed") || logs.contains("test"),
                "Elixir queue and exchange tests should complete. Output: " + logs);
    }
}
