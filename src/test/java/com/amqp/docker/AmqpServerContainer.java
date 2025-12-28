package com.amqp.docker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Future;

/**
 * Testcontainers wrapper for the AMQP Server Docker image.
 *
 * Supports both AMQP 0.9.1 (port 5672) and AMQP 1.0 (port 5671).
 */
public class AmqpServerContainer extends GenericContainer<AmqpServerContainer> {

    private static final Logger log = LoggerFactory.getLogger(AmqpServerContainer.class);

    public static final int AMQP_091_PORT = 5672;
    public static final int AMQP_10_PORT = 5671;
    public static final int MANAGEMENT_PORT = 15672;

    public static final String DEFAULT_USER = "guest";
    public static final String DEFAULT_PASSWORD = "guest";

    private static final String PRE_BUILT_IMAGE_NAME = "amqp-server-test:latest";

    private static Boolean dockerAvailable = null;
    private static Boolean preBuiltImageAvailable = null;

    /**
     * Check if Docker is available on this system.
     */
    public static boolean isDockerAvailable() {
        if (dockerAvailable == null) {
            try {
                DockerClientFactory.instance().client();
                dockerAvailable = true;
                log.info("Docker is available");
            } catch (Exception e) {
                dockerAvailable = false;
                log.warn("Docker is not available: {}", e.getMessage());
            }
        }
        return dockerAvailable;
    }

    /**
     * Create container from pre-built image.
     */
    public AmqpServerContainer(String imageName) {
        super(DockerImageName.parse(imageName));
        configure();
    }

    /**
     * Create container from DockerImageName.
     */
    public AmqpServerContainer(DockerImageName imageName) {
        super(imageName);
        configure();
    }

    /**
     * Create container from ImageFromDockerfile (builds from Dockerfile).
     */
    public AmqpServerContainer(Future<String> image) {
        super(image);
        configure();
    }


    /**
     * Static factory method to create the container.
     * Uses pre-built image if available, otherwise builds from Dockerfile.
     */
    public static AmqpServerContainer createContainer() {
        if (isPreBuiltImageAvailable()) {
            log.info("Using pre-built Docker image: {}", PRE_BUILT_IMAGE_NAME);
            return new AmqpServerContainer(DockerImageName.parse(PRE_BUILT_IMAGE_NAME));
        }

        log.info("Pre-built image not found, building from Dockerfile");
        return new AmqpServerContainer(createImageFromDockerfile());
    }

    private static boolean isPreBuiltImageAvailable() {
        if (preBuiltImageAvailable == null) {
            try {
                var dockerClient = DockerClientFactory.instance().client();
                var images = dockerClient.listImagesCmd()
                        .withImageNameFilter("amqp-server-test")
                        .exec();
                preBuiltImageAvailable = images != null && !images.isEmpty();
                log.info("Pre-built image check: {} image(s) found",
                        images != null ? images.size() : 0);
            } catch (Exception e) {
                log.debug("Could not check for pre-built image: {}", e.getMessage());
                preBuiltImageAvailable = false;
            }
        }
        return preBuiltImageAvailable;
    }

    private static ImageFromDockerfile createImageFromDockerfile() {
        Path projectRoot = findProjectRoot();
        Path dockerfile = projectRoot.resolve("Dockerfile");
        log.info("Building Docker image from: {}", dockerfile.toAbsolutePath());

        return new ImageFromDockerfile("amqp-server-test", false)
                .withDockerfile(dockerfile)
                .withFileFromPath(".", projectRoot);
    }

    private static Path findProjectRoot() {
        // Try multiple strategies to find the project root

        // 1. Check if we're in the project directory (pom.xml exists)
        File currentDir = new File(System.getProperty("user.dir"));
        if (new File(currentDir, "pom.xml").exists() && new File(currentDir, "Dockerfile").exists()) {
            return currentDir.toPath();
        }

        // 2. Navigate up from current directory looking for pom.xml and Dockerfile
        File dir = currentDir;
        for (int i = 0; i < 5; i++) {
            if (new File(dir, "pom.xml").exists() && new File(dir, "Dockerfile").exists()) {
                return dir.toPath();
            }
            dir = dir.getParentFile();
            if (dir == null) break;
        }

        // 3. Try common Maven project locations relative to target/test-classes
        String classPath = AmqpServerContainer.class.getProtectionDomain()
                .getCodeSource().getLocation().getPath();
        // Handle Windows paths that start with /C:/
        if (classPath.matches("/[A-Za-z]:/.*")) {
            classPath = classPath.substring(1);
        }
        File classFile = new File(classPath);
        // Go up from target/test-classes to project root
        File projectDir = classFile.getParentFile();
        if (projectDir != null) projectDir = projectDir.getParentFile();
        if (projectDir != null && new File(projectDir, "pom.xml").exists()) {
            return projectDir.toPath();
        }

        // Fallback to current directory
        log.warn("Could not find project root, using current directory: {}", currentDir);
        return currentDir.toPath();
    }

    protected void configure() {
        withExposedPorts(AMQP_091_PORT, AMQP_10_PORT, MANAGEMENT_PORT);
        withEnv("AMQP_GUEST_USER", "true");
        withLogConsumer(new Slf4jLogConsumer(log));
        // Wait for both AMQP 0-9-1 and AMQP 1.0 servers to start
        // The server logs separate messages for each protocol
        WaitAllStrategy waitStrategy = new WaitAllStrategy()
                .withStrategy(Wait.forLogMessage(".*AMQP Server started on port.*", 1))
                .withStrategy(Wait.forLogMessage(".*AMQP 1\\.0 server started on port.*", 1))
                .withStartupTimeout(Duration.ofMinutes(2));
        waitingFor(waitStrategy);
    }

    /**
     * Get the mapped AMQP 0.9.1 port.
     */
    public int getAmqp091Port() {
        return getMappedPort(AMQP_091_PORT);
    }

    /**
     * Get the mapped AMQP 1.0 port.
     */
    public int getAmqp10Port() {
        return getMappedPort(AMQP_10_PORT);
    }

    /**
     * Get AMQP 0.9.1 connection URL.
     */
    public String getAmqp091Url() {
        return String.format("amqp://%s:%s@%s:%d/",
                DEFAULT_USER, DEFAULT_PASSWORD, getHost(), getAmqp091Port());
    }

    /**
     * Get AMQP 1.0 connection URL.
     */
    public String getAmqp10Url() {
        return String.format("amqp://%s:%d", getHost(), getAmqp10Port());
    }

    /**
     * Get the host for connections.
     */
    @Override
    public String getHost() {
        return super.getHost();
    }
}
