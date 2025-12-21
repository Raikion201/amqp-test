package com.amqp.external.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Programmatic wrapper for RabbitMQ PerfTest.
 *
 * Allows running performance tests from Java code.
 */
public class PerfTestRunner {

    private static final Logger log = LoggerFactory.getLogger(PerfTestRunner.class);

    private String uri;
    private int producers = 1;
    private int consumers = 1;
    private int rate = 0; // 0 = unlimited
    private int duration = 60; // seconds
    private int messageSize = 256;
    private int confirm = -1;
    private int multiAck = 0;
    private String queueName;
    private boolean autoDelete = true;
    private boolean jsonBody = false;

    public PerfTestRunner(String uri) {
        this.uri = uri;
    }

    public PerfTestRunner producers(int count) {
        this.producers = count;
        return this;
    }

    public PerfTestRunner consumers(int count) {
        this.consumers = count;
        return this;
    }

    public PerfTestRunner rate(int messagesPerSecond) {
        this.rate = messagesPerSecond;
        return this;
    }

    public PerfTestRunner duration(int seconds) {
        this.duration = seconds;
        return this;
    }

    public PerfTestRunner messageSize(int bytes) {
        this.messageSize = bytes;
        return this;
    }

    public PerfTestRunner confirm(int batchSize) {
        this.confirm = batchSize;
        return this;
    }

    public PerfTestRunner multiAck(int count) {
        this.multiAck = count;
        return this;
    }

    public PerfTestRunner queue(String name) {
        this.queueName = name;
        return this;
    }

    public PerfTestRunner autoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
        return this;
    }

    public PerfTestRunner jsonBody(boolean json) {
        this.jsonBody = json;
        return this;
    }

    /**
     * Build the command line arguments for PerfTest.
     */
    public List<String> buildArgs() {
        List<String> args = new ArrayList<>();

        args.add("--uri");
        args.add(uri);

        args.add("--producers");
        args.add(String.valueOf(producers));

        args.add("--consumers");
        args.add(String.valueOf(consumers));

        if (rate > 0) {
            args.add("--rate");
            args.add(String.valueOf(rate));
        }

        args.add("--time");
        args.add(String.valueOf(duration));

        args.add("--size");
        args.add(String.valueOf(messageSize));

        if (confirm > 0) {
            args.add("--confirm");
            args.add(String.valueOf(confirm));
        }

        if (multiAck > 0) {
            args.add("--multi-ack-every");
            args.add(String.valueOf(multiAck));
        }

        if (queueName != null) {
            args.add("--queue");
            args.add(queueName);
        }

        if (autoDelete) {
            args.add("--auto-delete");
            args.add("true");
        }

        if (jsonBody) {
            args.add("--json-body");
        }

        return args;
    }

    /**
     * Run the performance test using Docker.
     */
    public PerfTestResult runWithDocker() {
        log.info("Running PerfTest with Docker");
        log.info("Arguments: {}", buildArgs());

        // This would execute the docker command
        // For now, return a placeholder result
        return new PerfTestResult();
    }

    /**
     * Performance test result.
     */
    public static class PerfTestResult {
        private long messagesSent;
        private long messagesReceived;
        private double publishRate;
        private double consumeRate;
        private double latencyMin;
        private double latencyMax;
        private double latencyAvg;
        private double latency99th;

        // Getters and setters
        public long getMessagesSent() { return messagesSent; }
        public void setMessagesSent(long messagesSent) { this.messagesSent = messagesSent; }

        public long getMessagesReceived() { return messagesReceived; }
        public void setMessagesReceived(long messagesReceived) { this.messagesReceived = messagesReceived; }

        public double getPublishRate() { return publishRate; }
        public void setPublishRate(double publishRate) { this.publishRate = publishRate; }

        public double getConsumeRate() { return consumeRate; }
        public void setConsumeRate(double consumeRate) { this.consumeRate = consumeRate; }

        public double getLatencyMin() { return latencyMin; }
        public void setLatencyMin(double latencyMin) { this.latencyMin = latencyMin; }

        public double getLatencyMax() { return latencyMax; }
        public void setLatencyMax(double latencyMax) { this.latencyMax = latencyMax; }

        public double getLatencyAvg() { return latencyAvg; }
        public void setLatencyAvg(double latencyAvg) { this.latencyAvg = latencyAvg; }

        public double getLatency99th() { return latency99th; }
        public void setLatency99th(double latency99th) { this.latency99th = latency99th; }

        @Override
        public String toString() {
            return String.format(
                    "PerfTestResult{sent=%d, received=%d, pubRate=%.2f msg/s, consumeRate=%.2f msg/s, latencyAvg=%.2f ms}",
                    messagesSent, messagesReceived, publishRate, consumeRate, latencyAvg);
        }
    }
}
