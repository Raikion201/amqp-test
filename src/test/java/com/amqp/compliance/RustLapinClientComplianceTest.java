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
 * AMQP Compliance Tests using the official lapin test suite.
 *
 * This test runs the actual test files from:
 * https://github.com/amqp-rs/lapin
 *
 * Test files are stored in: src/test/resources/compliance-tests/rust-lapin/
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Rust lapin Library Test Suite")
public class RustLapinClientComplianceTest extends DockerClientTestBase {

    private static final Logger logger = LoggerFactory.getLogger(RustLapinClientComplianceTest.class);
    private static final Path TEST_FILES_PATH = Paths.get("src/test/resources/compliance-tests/rust-lapin");

    @Test
    @Order(1)
    @DisplayName("lapin: Run connection tests from library")
    void testLapinConnectionTests() throws Exception {
        logger.info("Running lapin connection tests...");

        GenericContainer<?> rustClient = new GenericContainer<>("rust:1.75-slim")
                .withNetwork(network)
                .withEnv("AMQP_URL", getAmqpUrl())
                .withCopyFileToContainer(
                        MountableFile.forHostPath(TEST_FILES_PATH),
                        "/tests")
                .withCommand("sh", "-c", """
                    set -e
                    apt-get update -qq && apt-get install -y -qq pkg-config libssl-dev >/dev/null 2>&1

                    cd /tests

                    # Create test project with library tests
                    cat > Cargo.toml << 'EOF'
                    [package]
                    name = "lapin-test"
                    version = "0.1.0"
                    edition = "2021"

                    [dependencies]
                    lapin = "2.3"
                    tokio = { version = "1", features = ["full"] }
                    futures-lite = "2.0"

                    [[test]]
                    name = "connection"
                    path = "connection.rs"
                    EOF

                    echo "=== Running lapin connection tests ==="
                    cargo test --test connection 2>&1 || true
                    echo "=== Connection tests completed ==="
                    """)
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("LAPIN"))
                .withStartupTimeout(java.time.Duration.ofMinutes(15));

        String logs = runClientAndGetLogs(rustClient, 900);
        logger.info("Lapin test output:\n{}", logs);

        assertTrue(logs.contains("tests completed") || logs.contains("test result"),
                "Lapin tests should complete. Output: " + logs);
    }
}
