package com.amqp.compliance;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Base class for Docker-based multi-language AMQP client compliance tests.
 *
 * This test harness:
 * 1. Builds and starts our AMQP server in a Docker container
 * 2. Runs client containers from various languages against it
 * 3. Verifies the results
 *
 * Each language-specific test extends this class and provides its own
 * client container configuration.
 */
public abstract class DockerClientTestBase {

    protected static final Logger logger = LoggerFactory.getLogger(DockerClientTestBase.class);

    protected static final int AMQP_PORT = 5672;
    protected static final String AMQP_HOST = "amqp-server";
    protected static final String USERNAME = "guest";
    protected static final String PASSWORD = "guest";
    protected static final String VHOST = "/";

    protected static Network network;
    protected static GenericContainer<?> amqpServer;

    private static final Path PROJECT_ROOT = Paths.get(System.getProperty("user.dir"));

    @BeforeAll
    static void startAmqpServer() throws Exception {
        logger.info("=== Starting AMQP Server Container ===");

        // Check if Docker is available
        boolean dockerAvailable = isDockerAvailable();
        assumeTrue(dockerAvailable, "Docker is not available. Skipping Docker-based tests.");

        // Create shared network
        network = Network.newNetwork();

        // Build and start AMQP server container (deleteOnExit=true forces rebuild)
        amqpServer = new GenericContainer<>(
                new ImageFromDockerfile("amqp-server-test", true)
                        .withDockerfile(PROJECT_ROOT.resolve("Dockerfile")))
                .withNetwork(network)
                .withNetworkAliases(AMQP_HOST)
                .withExposedPorts(AMQP_PORT, 5671, 15672)
                .withEnv("AMQP_GUEST_USER", "true")
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("AMQP-SERVER"))
                .waitingFor(Wait.forLogMessage(".*AMQP Server started on port.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));

        amqpServer.start();

        logger.info("AMQP Server started on port {}", amqpServer.getMappedPort(AMQP_PORT));
    }

    /**
     * Check if Docker is available and running.
     */
    private static boolean isDockerAvailable() {
        try {
            DockerClientFactory.instance().client();
            return true;
        } catch (Exception e) {
            logger.warn("Docker is not available: {}", e.getMessage());
            return false;
        }
    }

    @AfterAll
    static void stopAmqpServer() {
        logger.info("=== Stopping AMQP Server Container ===");

        if (amqpServer != null) {
            amqpServer.stop();
        }
        if (network != null) {
            network.close();
        }

        logger.info("AMQP Server stopped");
    }

    /**
     * Get the AMQP server host for client containers (network alias).
     */
    protected String getAmqpHost() {
        return AMQP_HOST;
    }

    /**
     * Get the mapped AMQP port for external access.
     */
    protected int getMappedAmqpPort() {
        return amqpServer.getMappedPort(AMQP_PORT);
    }

    /**
     * Get the AMQP connection URL for client containers.
     */
    protected String getAmqpUrl() {
        return String.format("amqp://%s:%s@%s:%d/%s",
                USERNAME, PASSWORD, AMQP_HOST, AMQP_PORT, VHOST);
    }

    /**
     * Get the AMQP connection URL for localhost access (mapped port).
     */
    protected String getLocalAmqpUrl() {
        return String.format("amqp://%s:%s@localhost:%d/%s",
                USERNAME, PASSWORD, getMappedAmqpPort(), VHOST);
    }

    /**
     * Create a client container with the given image and test command.
     */
    protected GenericContainer<?> createClientContainer(String imageName, String... command) {
        return new GenericContainer<>(DockerImageName.parse(imageName))
                .withNetwork(network)
                .withCommand(command)
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix(imageName.toUpperCase()));
    }

    /**
     * Create a client container from a Dockerfile.
     */
    protected GenericContainer<?> createClientContainer(Path dockerfile, String imageName, String... command) {
        return new GenericContainer<>(
                new ImageFromDockerfile(imageName, false)
                        .withDockerfile(dockerfile))
                .withNetwork(network)
                .withCommand(command)
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix(imageName.toUpperCase()));
    }

    /**
     * Run a client container and wait for it to complete.
     * Returns the exit code.
     */
    protected int runClientContainer(GenericContainer<?> client) {
        try {
            client.start();

            // Wait for container to finish
            while (client.isRunning()) {
                Thread.sleep(100);
            }

            // Get exit code
            return client.getCurrentContainerInfo()
                    .getState()
                    .getExitCodeLong()
                    .intValue();

        } catch (Exception e) {
            logger.error("Error running client container", e);
            return -1;
        } finally {
            client.stop();
        }
    }

    /**
     * Run a client container and return its logs.
     */
    protected String runClientAndGetLogs(GenericContainer<?> client) {
        try {
            client.start();

            // Wait for container to finish
            while (client.isRunning()) {
                Thread.sleep(100);
            }

            return client.getLogs();

        } catch (Exception e) {
            logger.error("Error running client container", e);
            return "ERROR: " + e.getMessage();
        } finally {
            client.stop();
        }
    }

    /**
     * Get the path to language-specific client resources.
     */
    protected Path getClientResourcePath(String language) {
        return PROJECT_ROOT.resolve("src/test/resources/docker-clients").resolve(language);
    }
}
