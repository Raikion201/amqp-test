package com.amqp.management;

import com.amqp.model.Exchange;
import com.amqp.model.Queue;
import com.amqp.model.EnhancedQueue;
import com.amqp.security.AuthenticationManager;
import com.amqp.security.User;
import com.amqp.security.VirtualHost;
import com.amqp.consumer.ConsumerManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ManagementApi {
    private static final Logger logger = LoggerFactory.getLogger(ManagementApi.class);

    private final AuthenticationManager authManager;
    private final ConsumerManager consumerManager;
    private final ObjectMapper objectMapper;
    private final Statistics statistics;

    public ManagementApi(AuthenticationManager authManager, ConsumerManager consumerManager) {
        this.authManager = authManager;
        this.consumerManager = consumerManager;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.statistics = new Statistics();
    }

    // Overview
    public Map<String, Object> getOverview() {
        Map<String, Object> overview = new HashMap<>();

        overview.put("rabbitmq_version", "3.12.0-compatible");
        overview.put("cluster_name", "amcp@localhost");
        overview.put("management_version", "1.0.0");

        List<VirtualHost> vhosts = authManager.getAllVirtualHosts();
        int totalQueues = 0;
        int totalExchanges = 0;
        int totalConnections = 0;

        for (VirtualHost vhost : vhosts) {
            totalQueues += vhost.getQueueCount();
            totalExchanges += vhost.getExchangeCount();
        }

        Map<String, Object> objectTotals = new HashMap<>();
        objectTotals.put("consumers", consumerManager.getConsumerCount());
        objectTotals.put("queues", totalQueues);
        objectTotals.put("exchanges", totalExchanges);
        objectTotals.put("connections", totalConnections);
        objectTotals.put("channels", 0);

        overview.put("object_totals", objectTotals);
        overview.put("statistics", statistics.getGlobalStatistics());

        return overview;
    }

    // Virtual Hosts
    public List<Map<String, Object>> getVirtualHosts() {
        return authManager.getAllVirtualHosts().stream()
            .map(this::vhostToMap)
            .collect(Collectors.toList());
    }

    public Map<String, Object> getVirtualHost(String name) {
        VirtualHost vhost = authManager.getVirtualHost(name);
        if (vhost == null) {
            throw new IllegalArgumentException("Virtual host not found: " + name);
        }
        return vhostToMap(vhost);
    }

    public void createVirtualHost(String name) {
        authManager.createVirtualHost(name);
        logger.info("Created virtual host via API: {}", name);
    }

    public void deleteVirtualHost(String name) {
        authManager.deleteVirtualHost(name);
        logger.info("Deleted virtual host via API: {}", name);
    }

    // Users
    public List<Map<String, Object>> getUsers() {
        return authManager.getAllUsers().stream()
            .map(this::userToMap)
            .collect(Collectors.toList());
    }

    public Map<String, Object> getUser(String username) {
        User user = authManager.getUser(username);
        if (user == null) {
            throw new IllegalArgumentException("User not found: " + username);
        }
        return userToMap(user);
    }

    public void createUser(String username, String password, Set<String> tags) {
        authManager.createUser(username, password, tags);
        logger.info("Created user via API: {}", username);
    }

    public void deleteUser(String username) {
        authManager.deleteUser(username);
        logger.info("Deleted user via API: {}", username);
    }

    public void setUserPermissions(String username, String vhost,
                                   String configure, String write, String read) {
        authManager.setUserPermissions(username, vhost, configure, write, read);
        logger.info("Set permissions for user {} on vhost {} via API", username, vhost);
    }

    // Exchanges
    public List<Map<String, Object>> getExchanges(String vhostName) {
        VirtualHost vhost = authManager.getVirtualHost(vhostName);
        if (vhost == null) {
            return Collections.emptyList();
        }

        return vhost.getAllExchanges().stream()
            .map(this::exchangeToMap)
            .collect(Collectors.toList());
    }

    public Map<String, Object> getExchange(String vhostName, String exchangeName) {
        VirtualHost vhost = authManager.getVirtualHost(vhostName);
        if (vhost == null) {
            throw new IllegalArgumentException("Virtual host not found: " + vhostName);
        }

        Exchange exchange = vhost.getExchange(exchangeName);
        if (exchange == null) {
            throw new IllegalArgumentException("Exchange not found: " + exchangeName);
        }

        return exchangeToMap(exchange);
    }

    // Queues
    public List<Map<String, Object>> getQueues(String vhostName) {
        VirtualHost vhost = authManager.getVirtualHost(vhostName);
        if (vhost == null) {
            return Collections.emptyList();
        }

        return vhost.getAllQueues().stream()
            .map(this::queueToMap)
            .collect(Collectors.toList());
    }

    public Map<String, Object> getQueue(String vhostName, String queueName) {
        VirtualHost vhost = authManager.getVirtualHost(vhostName);
        if (vhost == null) {
            throw new IllegalArgumentException("Virtual host not found: " + vhostName);
        }

        Queue queue = vhost.getQueue(queueName);
        if (queue == null) {
            throw new IllegalArgumentException("Queue not found: " + queueName);
        }

        return queueToMap(queue);
    }

    // Consumers
    public List<Map<String, Object>> getConsumers() {
        return consumerManager.getAllConsumers().stream()
            .map(this::consumerToMap)
            .collect(Collectors.toList());
    }

    public List<Map<String, Object>> getConsumersForQueue(String vhostName, String queueName) {
        return consumerManager.getConsumersForQueue(queueName).stream()
            .map(this::consumerToMap)
            .collect(Collectors.toList());
    }

    // Health Check
    public Map<String, Object> healthCheck() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "ok");
        health.put("timestamp", System.currentTimeMillis());

        List<VirtualHost> vhosts = authManager.getAllVirtualHosts();
        health.put("vhosts", vhosts.size());
        health.put("queues", vhosts.stream().mapToInt(VirtualHost::getQueueCount).sum());
        health.put("exchanges", vhosts.stream().mapToInt(VirtualHost::getExchangeCount).sum());
        health.put("consumers", consumerManager.getConsumerCount());

        return health;
    }

    // Helper methods
    private Map<String, Object> vhostToMap(VirtualHost vhost) {
        Map<String, Object> map = new HashMap<>();
        map.put("name", vhost.getName());
        map.put("tracing", vhost.isTracing());
        map.put("messages", 0);
        map.put("messages_ready", 0);
        map.put("messages_unacknowledged", 0);
        return map;
    }

    private Map<String, Object> userToMap(User user) {
        Map<String, Object> map = new HashMap<>();
        map.put("name", user.getUsername());
        map.put("tags", user.getTags());
        map.put("password_hash", "");  // Don't expose password hash
        return map;
    }

    private Map<String, Object> exchangeToMap(Exchange exchange) {
        Map<String, Object> map = new HashMap<>();
        map.put("name", exchange.getName());
        map.put("type", exchange.getType().toString().toLowerCase());
        map.put("durable", exchange.isDurable());
        map.put("auto_delete", exchange.isAutoDelete());
        map.put("internal", exchange.isInternal());

        Map<String, Object> arguments = new HashMap<>();
        if (exchange.hasAlternateExchange()) {
            arguments.put("alternate-exchange", exchange.getAlternateExchange());
        }
        map.put("arguments", arguments);

        return map;
    }

    private Map<String, Object> queueToMap(Queue queue) {
        Map<String, Object> map = new HashMap<>();
        map.put("name", queue.getName());
        map.put("durable", queue.isDurable());
        map.put("auto_delete", queue.isAutoDelete());
        map.put("exclusive", queue.isExclusive());
        map.put("messages", queue.size());
        map.put("messages_ready", queue.size());
        map.put("messages_unacknowledged", 0);
        map.put("consumers", consumerManager.getConsumerCountForQueue(queue.getName()));

        if (queue instanceof EnhancedQueue) {
            EnhancedQueue eq = (EnhancedQueue) queue;
            map.put("arguments", eq.getArguments().toMap());
            map.put("memory", eq.getTotalBytesSize());
        } else {
            map.put("arguments", Collections.emptyMap());
        }

        return map;
    }

    private Map<String, Object> consumerToMap(ConsumerManager.Consumer consumer) {
        Map<String, Object> map = new HashMap<>();
        map.put("consumer_tag", consumer.getConsumerTag());
        map.put("queue", consumer.getQueueName());
        map.put("channel_number", consumer.getChannelNumber());
        map.put("ack_required", !consumer.isNoAck());
        map.put("exclusive", consumer.isExclusive());
        map.put("arguments", consumer.getArguments());
        return map;
    }

    public Statistics getStatistics() {
        return statistics;
    }
}
