package com.amqp.security.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Audit logger for security events.
 *
 * Logs events to configured destinations (file, console, external system).
 */
public class AuditLogger {

    private static final Logger log = LoggerFactory.getLogger(AuditLogger.class);
    private static final Logger auditLog = LoggerFactory.getLogger("AUDIT");

    private final AuditConfig config;
    private final List<Consumer<AuditEvent>> listeners = new CopyOnWriteArrayList<>();
    private final BlockingQueue<AuditEvent> eventQueue;
    private final ExecutorService executor;

    public AuditLogger(AuditConfig config) {
        this.config = config;
        this.eventQueue = new LinkedBlockingQueue<>(config.getQueueSize());

        if (config.isAsync()) {
            this.executor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "audit-logger");
                t.setDaemon(true);
                return t;
            });
            this.executor.submit(this::processEvents);
        } else {
            this.executor = null;
        }
    }

    /**
     * Log an audit event.
     */
    public void log(AuditEvent event) {
        if (!config.isEnabled()) {
            return;
        }

        // Filter by event type
        if (!config.isEventTypeEnabled(event.getType())) {
            return;
        }

        if (config.isAsync()) {
            if (!eventQueue.offer(event)) {
                log.warn("Audit queue full, dropping event: {}", event.getType());
            }
        } else {
            processEvent(event);
        }
    }

    private void processEvents() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                AuditEvent event = eventQueue.take();
                processEvent(event);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error processing audit event", e);
            }
        }
    }

    private void processEvent(AuditEvent event) {
        try {
            // Log to audit logger
            if (config.isLogToFile()) {
                String message = formatEvent(event);
                if (event.isSuccess()) {
                    auditLog.info(message);
                } else {
                    auditLog.warn(message);
                }
            }

            // Log to console if configured
            if (config.isLogToConsole()) {
                log.info("AUDIT: {}", event);
            }

            // Notify listeners
            for (Consumer<AuditEvent> listener : listeners) {
                try {
                    listener.accept(event);
                } catch (Exception e) {
                    log.error("Error in audit listener", e);
                }
            }

        } catch (Exception e) {
            log.error("Error processing audit event", e);
        }
    }

    private String formatEvent(AuditEvent event) {
        switch (config.getFormat()) {
            case JSON:
                return formatJson(event);
            case CSV:
                return formatCsv(event);
            case TEXT:
            default:
                return event.toString();
        }
    }

    private String formatJson(AuditEvent event) {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"timestamp\":\"").append(event.getTimestamp()).append("\",");
        sb.append("\"type\":\"").append(event.getType()).append("\",");
        sb.append("\"success\":").append(event.isSuccess());

        if (event.getUsername() != null) {
            sb.append(",\"username\":\"").append(escapeJson(event.getUsername())).append("\"");
        }
        if (event.getIpAddress() != null) {
            sb.append(",\"ipAddress\":\"").append(escapeJson(event.getIpAddress())).append("\"");
        }
        if (event.getResource() != null) {
            sb.append(",\"resource\":\"").append(escapeJson(event.getResource())).append("\"");
        }
        if (event.getAction() != null) {
            sb.append(",\"action\":\"").append(escapeJson(event.getAction())).append("\"");
        }
        if (event.getMessage() != null) {
            sb.append(",\"message\":\"").append(escapeJson(event.getMessage())).append("\"");
        }

        sb.append("}");
        return sb.toString();
    }

    private String formatCsv(AuditEvent event) {
        return String.join(",",
                event.getTimestamp().toString(),
                event.getType().name(),
                String.valueOf(event.isSuccess()),
                escapeCsv(event.getUsername()),
                escapeCsv(event.getIpAddress()),
                escapeCsv(event.getResource()),
                escapeCsv(event.getAction()),
                escapeCsv(event.getMessage())
        );
    }

    private String escapeJson(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private String escapeCsv(String value) {
        if (value == null) return "";
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    /**
     * Add a listener for audit events.
     */
    public void addListener(Consumer<AuditEvent> listener) {
        listeners.add(listener);
    }

    /**
     * Remove a listener.
     */
    public void removeListener(Consumer<AuditEvent> listener) {
        listeners.remove(listener);
    }

    /**
     * Get pending event count.
     */
    public int getPendingEventCount() {
        return eventQueue.size();
    }

    public void shutdown() {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
    }

    // Convenience methods
    public void logAuthSuccess(String username, String ipAddress) {
        log(AuditEvent.authSuccess(username, ipAddress));
    }

    public void logAuthFailure(String username, String ipAddress, String reason) {
        log(AuditEvent.authFailure(username, ipAddress, reason));
    }

    public void logConnectionOpened(String username, String ipAddress) {
        log(AuditEvent.connectionOpened(username, ipAddress));
    }

    public void logConnectionRejected(String ipAddress, String reason) {
        log(AuditEvent.connectionRejected(ipAddress, reason));
    }

    public void logAccessDenied(String username, String resource, String action) {
        log(AuditEvent.accessDenied(username, resource, action));
    }
}
