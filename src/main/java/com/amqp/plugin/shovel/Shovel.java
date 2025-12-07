package com.amqp.plugin.shovel;

import com.amqp.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shovel Plugin - forwards messages from a source queue to a destination exchange/queue.
 * Can operate within the same broker or between different brokers.
 *
 * Use cases:
 * - Forwarding messages between clusters
 * - Copying messages for backup/archival
 * - Message routing across network boundaries
 * - Load distribution
 */
public class Shovel {
    private static final Logger logger = LoggerFactory.getLogger(Shovel.class);

    private final String name;
    private final ShovelSource source;
    private final ShovelDestination destination;
    private final ShovelConfig config;

    private final AtomicBoolean running;
    private final ExecutorService executor;
    private volatile Future<?> shovelTask;

    private final AtomicLong messagesForwarded;
    private final AtomicLong bytesForwarded;
    private final AtomicLong errors;
    private volatile long lastForwardTime;

    public Shovel(String name, ShovelSource source, ShovelDestination destination,
                  ShovelConfig config) {
        this.name = name;
        this.source = source;
        this.destination = destination;
        this.config = config;

        this.running = new AtomicBoolean(false);
        this.executor = Executors.newSingleThreadExecutor(
            r -> new Thread(r, "shovel-" + name));

        this.messagesForwarded = new AtomicLong(0);
        this.bytesForwarded = new AtomicLong(0);
        this.errors = new AtomicLong(0);
    }

    /**
     * Start the shovel.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting shovel: {}", name);
            shovelTask = executor.submit(this::runShovel);
        }
    }

    /**
     * Stop the shovel.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping shovel: {}", name);

            if (shovelTask != null) {
                shovelTask.cancel(true);
            }

            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Main shovel loop.
     */
    private void runShovel() {
        logger.info("Shovel {} started", name);

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                // Consume from source
                Message message = source.consume();

                if (message == null) {
                    // No messages available, wait before retrying
                    Thread.sleep(config.getPrefetchInterval());
                    continue;
                }

                // Apply transformations if configured
                Message transformed = transform(message);

                // Publish to destination
                boolean published = destination.publish(transformed);

                if (published) {
                    // Acknowledge source message
                    source.acknowledge(message);

                    // Update stats
                    messagesForwarded.incrementAndGet();
                    bytesForwarded.addAndGet(calculateMessageSize(message));
                    lastForwardTime = System.currentTimeMillis();

                    logger.debug("Shovel {} forwarded message", name);
                } else {
                    // Failed to publish, handle based on config
                    handlePublishFailure(message);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in shovel {}", name, e);
                errors.incrementAndGet();

                try {
                    Thread.sleep(config.getReconnectDelay());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        logger.info("Shovel {} stopped", name);
    }

    /**
     * Transform message if transformations are configured.
     */
    private Message transform(Message message) {
        if (config.getTransformations() == null || config.getTransformations().isEmpty()) {
            return message;
        }

        // Apply transformations (add headers, modify routing key, etc.)
        Message transformed = message; // Would create a copy in real implementation

        for (Map.Entry<String, Object> transform : config.getTransformations().entrySet()) {
            String type = transform.getKey();
            Object value = transform.getValue();

            switch (type) {
                case "add-header":
                    if (value instanceof Map) {
                        Map<String, Object> headers = message.getHeaders();
                        headers.putAll((Map<String, Object>) value);
                    }
                    break;

                case "set-routing-key":
                    message.setRoutingKey(value.toString());
                    break;

                // Add more transformation types as needed
            }
        }

        return transformed;
    }

    /**
     * Handle publish failure based on configuration.
     */
    private void handlePublishFailure(Message message) {
        errors.incrementAndGet();

        String failureMode = config.getAckMode();
        switch (failureMode) {
            case "on-publish":
                // Don't acknowledge, message will be redelivered
                break;

            case "on-confirm":
                // Wait for publisher confirm (if supported)
                break;

            case "no-ack":
                // Acknowledge anyway, message is lost
                source.acknowledge(message);
                break;
        }
    }

    private long calculateMessageSize(Message message) {
        long size = message.getBody() != null ? message.getBody().length : 0;
        size += message.getHeaders() != null ? message.getHeaders().size() * 50 : 0;
        size += 200; // Overhead
        return size;
    }

    /**
     * Get shovel statistics.
     */
    public ShovelStats getStats() {
        return new ShovelStats(
            name,
            running.get(),
            messagesForwarded.get(),
            bytesForwarded.get(),
            errors.get(),
            lastForwardTime
        );
    }

    public String getName() {
        return name;
    }

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Shovel statistics.
     */
    public static class ShovelStats {
        public final String name;
        public final boolean running;
        public final long messagesForwarded;
        public final long bytesForwarded;
        public final long errors;
        public final long lastForwardTime;

        public ShovelStats(String name, boolean running, long messagesForwarded,
                          long bytesForwarded, long errors, long lastForwardTime) {
            this.name = name;
            this.running = running;
            this.messagesForwarded = messagesForwarded;
            this.bytesForwarded = bytesForwarded;
            this.errors = errors;
            this.lastForwardTime = lastForwardTime;
        }

        @Override
        public String toString() {
            return String.format("ShovelStats{name='%s', running=%s, forwarded=%d, bytes=%d, errors=%d}",
                               name, running, messagesForwarded, bytesForwarded, errors);
        }
    }

    /**
     * Shovel source interface.
     */
    public interface ShovelSource {
        Message consume() throws Exception;
        void acknowledge(Message message);
        void reject(Message message, boolean requeue);
    }

    /**
     * Shovel destination interface.
     */
    public interface ShovelDestination {
        boolean publish(Message message) throws Exception;
    }
}
