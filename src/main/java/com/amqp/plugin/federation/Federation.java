package com.amqp.plugin.federation;

import com.amqp.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Federation Plugin - allows exchanges and queues to receive messages from remote brokers.
 * This enables distributed messaging patterns across multiple RabbitMQ clusters.
 *
 * Federation links can be:
 * - Exchange federation: Receive messages published to a remote exchange
 * - Queue federation: Consume messages from a remote queue
 *
 * Key features:
 * - Automatic reconnection on failure
 * - Bidirectional message flow (with proper configuration)
 * - Loop prevention using message headers
 * - Configurable max hops
 */
public class Federation {
    private static final Logger logger = LoggerFactory.getLogger(Federation.class);

    private final String name;
    private final FederationType type;
    private final FederationConfig config;
    private final FederationUpstream upstream;

    private final AtomicBoolean running;
    private final ExecutorService executor;
    private volatile Future<?> federationTask;

    private final AtomicLong messagesReceived;
    private final AtomicLong bytesReceived;
    private final AtomicLong errors;
    private volatile long lastMessageTime;
    private volatile FederationStatus status;

    public enum FederationType {
        EXCHANGE,
        QUEUE
    }

    public enum FederationStatus {
        STARTING,
        RUNNING,
        STOPPED,
        ERROR
    }

    public Federation(String name, FederationType type, FederationUpstream upstream,
                     FederationConfig config) {
        this.name = name;
        this.type = type;
        this.upstream = upstream;
        this.config = config;

        this.running = new AtomicBoolean(false);
        this.executor = Executors.newSingleThreadExecutor(
            r -> new Thread(r, "federation-" + name));

        this.messagesReceived = new AtomicLong(0);
        this.bytesReceived = new AtomicLong(0);
        this.errors = new AtomicLong(0);
        this.status = FederationStatus.STOPPED;
    }

    /**
     * Start the federation link.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting federation link: {} (type: {})", name, type);
            status = FederationStatus.STARTING;
            federationTask = executor.submit(this::runFederation);
        }
    }

    /**
     * Stop the federation link.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping federation link: {}", name);
            status = FederationStatus.STOPPED;

            if (federationTask != null) {
                federationTask.cancel(true);
            }

            upstream.disconnect();

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
     * Main federation loop.
     */
    private void runFederation() {
        logger.info("Federation link {} started", name);

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                // Connect to upstream if not connected
                if (!upstream.isConnected()) {
                    status = FederationStatus.STARTING;
                    upstream.connect();
                    logger.info("Federation link {} connected to upstream", name);
                }

                status = FederationStatus.RUNNING;

                // Receive message from upstream
                Message message = upstream.receive(config.getReceiveTimeout());

                if (message == null) {
                    continue; // Timeout, retry
                }

                // Check for loops
                if (isLooping(message)) {
                    logger.warn("Message loop detected in federation link {}, dropping message", name);
                    upstream.acknowledge(message);
                    continue;
                }

                // Add federation headers
                addFederationHeaders(message);

                // Forward message locally
                boolean forwarded = forwardMessage(message);

                if (forwarded) {
                    upstream.acknowledge(message);

                    messagesReceived.incrementAndGet();
                    bytesReceived.addAndGet(calculateMessageSize(message));
                    lastMessageTime = System.currentTimeMillis();

                    logger.debug("Federation link {} received and forwarded message", name);
                } else {
                    upstream.reject(message, true); // Requeue on failure
                    errors.incrementAndGet();
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in federation link {}", name, e);
                errors.incrementAndGet();
                status = FederationStatus.ERROR;

                upstream.disconnect();

                try {
                    Thread.sleep(config.getReconnectDelay());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        logger.info("Federation link {} stopped", name);
    }

    /**
     * Check if message is looping (has been through too many federation hops).
     */
    private boolean isLooping(Message message) {
        Map<String, Object> headers = message.getHeaders();
        if (headers == null) {
            return false;
        }

        Object hopsObj = headers.get("x-federation-hops");
        if (hopsObj instanceof Number) {
            int hops = ((Number) hopsObj).intValue();
            return hops >= config.getMaxHops();
        }

        return false;
    }

    /**
     * Add federation headers to message.
     */
    private void addFederationHeaders(Message message) {
        Map<String, Object> headers = message.getHeaders();
        if (headers == null) {
            headers = new HashMap<>();
            message.setHeaders(headers);
        }

        // Increment hop count
        int hops = 0;
        Object hopsObj = headers.get("x-federation-hops");
        if (hopsObj instanceof Number) {
            hops = ((Number) hopsObj).intValue();
        }
        headers.put("x-federation-hops", hops + 1);

        // Add federation link name
        List<String> links = (List<String>) headers.get("x-federation-links");
        if (links == null) {
            links = new ArrayList<>();
            headers.put("x-federation-links", links);
        }
        links.add(name);

        // Add upstream URI (if configured to expose it)
        if (config.isExposeUri()) {
            headers.put("x-federation-upstream", upstream.getUri());
        }
    }

    /**
     * Forward message to local broker.
     * Implementation depends on federation type.
     */
    private boolean forwardMessage(Message message) {
        // This would be implemented by the specific federation link
        // For exchange federation: publish to local exchange
        // For queue federation: enqueue to local queue
        return true; // Simplified
    }

    private long calculateMessageSize(Message message) {
        long size = message.getBody() != null ? message.getBody().length : 0;
        size += message.getHeaders() != null ? message.getHeaders().size() * 50 : 0;
        size += 200;
        return size;
    }

    /**
     * Get federation statistics.
     */
    public FederationStats getStats() {
        return new FederationStats(
            name,
            type,
            status,
            upstream.isConnected(),
            messagesReceived.get(),
            bytesReceived.get(),
            errors.get(),
            lastMessageTime
        );
    }

    public String getName() {
        return name;
    }

    public FederationType getType() {
        return type;
    }

    public FederationStatus getStatus() {
        return status;
    }

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Federation statistics.
     */
    public static class FederationStats {
        public final String name;
        public final FederationType type;
        public final FederationStatus status;
        public final boolean connected;
        public final long messagesReceived;
        public final long bytesReceived;
        public final long errors;
        public final long lastMessageTime;

        public FederationStats(String name, FederationType type, FederationStatus status,
                              boolean connected, long messagesReceived, long bytesReceived,
                              long errors, long lastMessageTime) {
            this.name = name;
            this.type = type;
            this.status = status;
            this.connected = connected;
            this.messagesReceived = messagesReceived;
            this.bytesReceived = bytesReceived;
            this.errors = errors;
            this.lastMessageTime = lastMessageTime;
        }

        @Override
        public String toString() {
            return String.format("FederationStats{name='%s', type=%s, status=%s, connected=%s, received=%d}",
                               name, type, status, connected, messagesReceived);
        }
    }

    /**
     * Federation upstream interface - represents connection to remote broker.
     */
    public interface FederationUpstream {
        void connect() throws Exception;
        void disconnect();
        boolean isConnected();
        Message receive(long timeout) throws Exception;
        void acknowledge(Message message);
        void reject(Message message, boolean requeue);
        String getUri();
    }
}
