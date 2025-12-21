package com.amqp.external.rabbitmq;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance test scenarios using RabbitMQ PerfTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Disabled("Run manually or in CI with Docker")
public class PerfTestScenariosIT {

    private static final Logger log = LoggerFactory.getLogger(PerfTestScenariosIT.class);

    private static String amqpUri;

    @BeforeAll
    static void setup() {
        String host = System.getenv().getOrDefault("AMQP_HOST", "localhost");
        String port = System.getenv().getOrDefault("AMQP_PORT_091", "5672");
        String user = System.getenv().getOrDefault("AMQP_USER", "guest");
        String password = System.getenv().getOrDefault("AMQP_PASSWORD", "guest");

        amqpUri = String.format("amqp://%s:%s@%s:%s", user, password, host, port);
        log.info("Using AMQP URI: {}", amqpUri.replaceAll(password, "****"));
    }

    @Test
    @Order(1)
    @DisplayName("Baseline: Single producer, single consumer")
    void testBaseline() {
        PerfTestRunner runner = new PerfTestRunner(amqpUri)
                .producers(1)
                .consumers(1)
                .duration(30)
                .messageSize(256);

        log.info("Running baseline test...");
        PerfTestRunner.PerfTestResult result = runner.runWithDocker();

        assertNotNull(result);
        log.info("Baseline result: {}", result);
    }

    @Test
    @Order(2)
    @DisplayName("Throughput: Maximum rate with multiple producers/consumers")
    void testMaxThroughput() {
        PerfTestRunner runner = new PerfTestRunner(amqpUri)
                .producers(4)
                .consumers(4)
                .duration(60)
                .messageSize(256)
                .confirm(100)
                .multiAck(100);

        log.info("Running throughput test...");
        PerfTestRunner.PerfTestResult result = runner.runWithDocker();

        assertNotNull(result);
        log.info("Throughput result: {}", result);
    }

    @Test
    @Order(3)
    @DisplayName("Latency: Rate-limited with latency measurement")
    void testLatency() {
        PerfTestRunner runner = new PerfTestRunner(amqpUri)
                .producers(1)
                .consumers(1)
                .rate(1000)
                .duration(30)
                .messageSize(256);

        log.info("Running latency test...");
        PerfTestRunner.PerfTestResult result = runner.runWithDocker();

        assertNotNull(result);
        log.info("Latency result: {}", result);
    }

    @Test
    @Order(4)
    @DisplayName("Large messages: 64KB message size")
    void testLargeMessages() {
        PerfTestRunner runner = new PerfTestRunner(amqpUri)
                .producers(2)
                .consumers(2)
                .rate(100)
                .duration(30)
                .messageSize(65536);

        log.info("Running large message test...");
        PerfTestRunner.PerfTestResult result = runner.runWithDocker();

        assertNotNull(result);
        log.info("Large message result: {}", result);
    }

    @Test
    @Order(5)
    @DisplayName("High concurrency: Many producers and consumers")
    void testHighConcurrency() {
        PerfTestRunner runner = new PerfTestRunner(amqpUri)
                .producers(10)
                .consumers(10)
                .rate(5000)
                .duration(60)
                .messageSize(256);

        log.info("Running high concurrency test...");
        PerfTestRunner.PerfTestResult result = runner.runWithDocker();

        assertNotNull(result);
        log.info("High concurrency result: {}", result);
    }

    @Test
    @Order(6)
    @DisplayName("Persistence: Durable messages with confirms")
    void testPersistence() {
        PerfTestRunner runner = new PerfTestRunner(amqpUri)
                .producers(2)
                .consumers(2)
                .rate(1000)
                .duration(30)
                .messageSize(256)
                .confirm(10)
                .autoDelete(false);

        log.info("Running persistence test...");
        PerfTestRunner.PerfTestResult result = runner.runWithDocker();

        assertNotNull(result);
        log.info("Persistence result: {}", result);
    }
}
