package com.amqp.external.qpid;

import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Base class for Qpid Proton tests.
 *
 * Provides common setup and utilities for AMQP 1.0 tests.
 */
public abstract class QpidProtonTestBase {

    protected static final Logger log = LoggerFactory.getLogger(QpidProtonTestBase.class);

    protected static String amqpHost;
    protected static int amqpPort10;
    protected static int amqpPort091;
    protected static String amqpUser;
    protected static String amqpPassword;
    protected static String amqpUrl;

    @BeforeAll
    static void setupBase() {
        amqpHost = System.getenv().getOrDefault("AMQP_HOST", "localhost");
        amqpPort10 = Integer.parseInt(System.getenv().getOrDefault("AMQP_PORT_10", "5671"));
        amqpPort091 = Integer.parseInt(System.getenv().getOrDefault("AMQP_PORT_091", "5672"));
        amqpUser = System.getenv().getOrDefault("AMQP_USER", "guest");
        amqpPassword = System.getenv().getOrDefault("AMQP_PASSWORD", "guest");

        amqpUrl = String.format("amqp://%s:%s@%s:%d",
                amqpUser, amqpPassword, amqpHost, amqpPort10);

        log.info("Test configuration: host={}, port10={}, port091={}, user={}",
                amqpHost, amqpPort10, amqpPort091, amqpUser);
    }

    /**
     * Wait with timeout.
     */
    protected void waitFor(long timeout, TimeUnit unit) throws InterruptedException {
        Thread.sleep(unit.toMillis(timeout));
    }

    /**
     * Generate a unique queue name.
     */
    protected String generateQueueName() {
        return "test-queue-" + System.currentTimeMillis();
    }

    /**
     * Generate a unique exchange name.
     */
    protected String generateExchangeName() {
        return "test-exchange-" + System.currentTimeMillis();
    }
}
