package com.amqp.persistence;

import com.amqp.model.Exchange;
import com.amqp.model.Queue;
import com.amqp.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PersistenceManager {
    private static final Logger logger = LoggerFactory.getLogger(PersistenceManager.class);
    
    private final DatabaseManager databaseManager;
    private final ObjectMapper objectMapper;
    
    public PersistenceManager(DatabaseManager databaseManager) {
        this.databaseManager = databaseManager;
        this.objectMapper = new ObjectMapper();
    }
    
    public void saveExchange(String vhost, Exchange exchange) {
        String sql = """
            INSERT INTO exchanges (vhost, name, type, durable, auto_delete, internal)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (vhost, name) DO NOTHING
            """;

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, vhost);
            stmt.setString(2, exchange.getName());
            stmt.setString(3, exchange.getType().name());
            stmt.setBoolean(4, exchange.isDurable());
            stmt.setBoolean(5, exchange.isAutoDelete());
            stmt.setBoolean(6, exchange.isInternal());

            stmt.executeUpdate();
            logger.debug("Saved exchange: {} in vhost: {}", exchange.getName(), vhost);

        } catch (SQLException e) {
            logger.error("Failed to save exchange: {} in vhost: {}", exchange.getName(), vhost, e);
        }
    }

    public void saveQueue(String vhost, Queue queue) {
        String sql = """
            INSERT INTO queues (vhost, name, durable, exclusive, auto_delete)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (vhost, name) DO NOTHING
            """;

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, vhost);
            stmt.setString(2, queue.getName());
            stmt.setBoolean(3, queue.isDurable());
            stmt.setBoolean(4, queue.isExclusive());
            stmt.setBoolean(5, queue.isAutoDelete());

            stmt.executeUpdate();
            logger.debug("Saved queue: {} in vhost: {}", queue.getName(), vhost);

        } catch (SQLException e) {
            logger.error("Failed to save queue: {} in vhost: {}", queue.getName(), vhost, e);
        }
    }

    public void saveBinding(String vhost, String exchangeName, String queueName, String routingKey) {
        String sql = """
            INSERT INTO bindings (vhost, exchange_name, queue_name, routing_key)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (vhost, exchange_name, queue_name, routing_key) DO NOTHING
            """;

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, vhost);
            stmt.setString(2, exchangeName);
            stmt.setString(3, queueName);
            stmt.setString(4, routingKey);

            stmt.executeUpdate();
            logger.debug("Saved binding: {} -> {} ({}) in vhost: {}", exchangeName, queueName, routingKey, vhost);

        } catch (SQLException e) {
            logger.error("Failed to save binding: {} -> {} in vhost: {}", exchangeName, queueName, vhost, e);
        }
    }

    public void saveMessage(String vhost, String queueName, Message message) {
        String sql = """
            INSERT INTO messages (vhost, queue_name, routing_key, content_type, content_encoding,
                                headers, delivery_mode, priority, correlation_id, reply_to,
                                expiration, message_id, timestamp, type, user_id, app_id,
                                cluster_id, body)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS)) {

            stmt.setString(1, vhost);
            stmt.setString(2, queueName);
            stmt.setString(3, message.getRoutingKey());
            stmt.setString(4, message.getContentType());
            stmt.setString(5, message.getContentEncoding());
            stmt.setString(6, serializeHeaders(message.getHeaders()));
            stmt.setShort(7, message.getDeliveryMode());
            stmt.setShort(8, message.getPriority());
            stmt.setString(9, message.getCorrelationId());
            stmt.setString(10, message.getReplyTo());
            stmt.setString(11, message.getExpiration());
            stmt.setString(12, message.getMessageId());
            stmt.setLong(13, message.getTimestamp());
            stmt.setString(14, message.getType());
            stmt.setString(15, message.getUserId());
            stmt.setString(16, message.getAppId());
            stmt.setString(17, message.getClusterId());
            stmt.setBytes(18, message.getBody());

            stmt.executeUpdate();

            // Retrieve the auto-generated ID and set it on the message
            try (java.sql.ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    long generatedId = generatedKeys.getLong(1);
                    message.setId(generatedId);
                    logger.debug("Saved message to queue: {} in vhost: {} with ID: {}", queueName, vhost, generatedId);
                } else {
                    logger.warn("Failed to retrieve generated ID for message in queue: {} vhost: {}", queueName, vhost);
                }
            }

        } catch (SQLException e) {
            logger.error("Failed to save message to queue: {} in vhost: {}", queueName, vhost, e);
        }
    }

    /**
     * Load all durable exchanges for a given vhost from the database on startup.
     */
    public List<ExchangeData> loadExchanges(String vhost) {
        String sql = "SELECT name, type, durable, auto_delete, internal FROM exchanges WHERE vhost = ? AND durable = true";
        List<ExchangeData> exchanges = new ArrayList<>();

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, vhost);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                ExchangeData data = new ExchangeData(
                    rs.getString("name"),
                    rs.getString("type"),
                    rs.getBoolean("durable"),
                    rs.getBoolean("auto_delete"),
                    rs.getBoolean("internal")
                );
                exchanges.add(data);
            }

            logger.info("Loaded {} durable exchanges from vhost: {}", exchanges.size(), vhost);

        } catch (SQLException e) {
            logger.error("Failed to load exchanges from vhost: {}", vhost, e);
        }

        return exchanges;
    }

    /**
     * Load all durable queues for a given vhost from the database on startup.
     */
    public List<QueueData> loadQueues(String vhost) {
        String sql = "SELECT name, durable, exclusive, auto_delete FROM queues WHERE vhost = ? AND durable = true";
        List<QueueData> queues = new ArrayList<>();

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, vhost);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                QueueData data = new QueueData(
                    rs.getString("name"),
                    rs.getBoolean("durable"),
                    rs.getBoolean("exclusive"),
                    rs.getBoolean("auto_delete")
                );
                queues.add(data);
            }

            logger.info("Loaded {} durable queues from vhost: {}", queues.size(), vhost);

        } catch (SQLException e) {
            logger.error("Failed to load queues from vhost: {}", vhost, e);
        }

        return queues;
    }

    /**
     * Load all bindings for a given vhost from the database on startup.
     */
    public List<BindingData> loadBindings(String vhost) {
        String sql = "SELECT exchange_name, queue_name, routing_key FROM bindings WHERE vhost = ?";
        List<BindingData> bindings = new ArrayList<>();

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, vhost);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                BindingData data = new BindingData(
                    rs.getString("exchange_name"),
                    rs.getString("queue_name"),
                    rs.getString("routing_key")
                );
                bindings.add(data);
            }

            logger.info("Loaded {} bindings from vhost: {}", bindings.size(), vhost);

        } catch (SQLException e) {
            logger.error("Failed to load bindings from vhost: {}", vhost, e);
        }

        return bindings;
    }

    /**
     * Load persisted messages for a given queue and vhost from the database on startup.
     */
    public List<Message> loadMessages(String vhost, String queueName) {
        String sql = """
            SELECT id, routing_key, content_type, content_encoding, headers,
                   delivery_mode, priority, correlation_id, reply_to, expiration,
                   message_id, timestamp, type, user_id, app_id, cluster_id, body
            FROM messages
            WHERE vhost = ? AND queue_name = ?
            ORDER BY created_at ASC
            """;
        List<Message> messages = new ArrayList<>();

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, vhost);
            stmt.setString(2, queueName);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                Message message = new Message();
                message.setId(rs.getLong("id"));
                message.setRoutingKey(rs.getString("routing_key"));
                message.setContentType(rs.getString("content_type"));
                message.setContentEncoding(rs.getString("content_encoding"));
                message.setHeaders(deserializeHeaders(rs.getString("headers")));
                message.setDeliveryMode(rs.getShort("delivery_mode"));
                message.setPriority(rs.getShort("priority"));
                message.setCorrelationId(rs.getString("correlation_id"));
                message.setReplyTo(rs.getString("reply_to"));
                message.setExpiration(rs.getString("expiration"));
                message.setMessageId(rs.getString("message_id"));
                message.setTimestamp(rs.getLong("timestamp"));
                message.setType(rs.getString("type"));
                message.setUserId(rs.getString("user_id"));
                message.setAppId(rs.getString("app_id"));
                message.setClusterId(rs.getString("cluster_id"));
                message.setBody(rs.getBytes("body"));
                messages.add(message);
            }

            logger.info("Loaded {} messages for queue: {} in vhost: {}", messages.size(), queueName, vhost);

        } catch (SQLException e) {
            logger.error("Failed to load messages for queue: {} in vhost: {}", queueName, vhost, e);
        }

        return messages;
    }

    // Data transfer objects for recovery
    public static class ExchangeData {
        public final String name;
        public final String type;
        public final boolean durable;
        public final boolean autoDelete;
        public final boolean internal;

        public ExchangeData(String name, String type, boolean durable, boolean autoDelete, boolean internal) {
            this.name = name;
            this.type = type;
            this.durable = durable;
            this.autoDelete = autoDelete;
            this.internal = internal;
        }
    }

    public static class QueueData {
        public final String name;
        public final boolean durable;
        public final boolean exclusive;
        public final boolean autoDelete;

        public QueueData(String name, boolean durable, boolean exclusive, boolean autoDelete) {
            this.name = name;
            this.durable = durable;
            this.exclusive = exclusive;
            this.autoDelete = autoDelete;
        }
    }

    public static class BindingData {
        public final String exchangeName;
        public final String queueName;
        public final String routingKey;

        public BindingData(String exchangeName, String queueName, String routingKey) {
            this.exchangeName = exchangeName;
            this.queueName = queueName;
            this.routingKey = routingKey;
        }
    }
    
    public void deleteMessage(long messageId) {
        String sql = "DELETE FROM messages WHERE id = ?";
        
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setLong(1, messageId);
            stmt.executeUpdate();
            logger.debug("Deleted message: {}", messageId);
            
        } catch (SQLException e) {
            logger.error("Failed to delete message: {}", messageId, e);
        }
    }
    
    public List<Message> loadMessages(String queueName) {
        String sql = """
            SELECT id, routing_key, content_type, content_encoding, headers,
                   delivery_mode, priority, correlation_id, reply_to, expiration,
                   message_id, timestamp, type, user_id, app_id, cluster_id, body
            FROM messages WHERE queue_name = ? ORDER BY created_at
            """;
        
        List<Message> messages = new ArrayList<>();
        
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, queueName);
            ResultSet rs = stmt.executeQuery();
            
            while (rs.next()) {
                Message message = new Message();
                message.setId(rs.getLong("id"));
                message.setRoutingKey(rs.getString("routing_key"));
                message.setContentType(rs.getString("content_type"));
                message.setContentEncoding(rs.getString("content_encoding"));
                message.setHeaders(deserializeHeaders(rs.getString("headers")));
                message.setDeliveryMode(rs.getShort("delivery_mode"));
                message.setPriority(rs.getShort("priority"));
                message.setCorrelationId(rs.getString("correlation_id"));
                message.setReplyTo(rs.getString("reply_to"));
                message.setExpiration(rs.getString("expiration"));
                message.setMessageId(rs.getString("message_id"));
                message.setTimestamp(rs.getLong("timestamp"));
                message.setType(rs.getString("type"));
                message.setUserId(rs.getString("user_id"));
                message.setAppId(rs.getString("app_id"));
                message.setClusterId(rs.getString("cluster_id"));
                message.setBody(rs.getBytes("body"));
                
                messages.add(message);
            }
            
            logger.debug("Loaded {} messages for queue: {}", messages.size(), queueName);
            
        } catch (SQLException e) {
            logger.error("Failed to load messages for queue: {}", queueName, e);
        }
        
        return messages;
    }
    
    private String serializeHeaders(Map<String, Object> headers) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        
        try {
            return objectMapper.writeValueAsString(headers);
        } catch (Exception e) {
            logger.warn("Failed to serialize headers", e);
            return null;
        }
    }
    
    @SuppressWarnings("unchecked")
    private Map<String, Object> deserializeHeaders(String headersJson) {
        if (headersJson == null || headersJson.trim().isEmpty()) {
            return null;
        }
        
        try {
            return objectMapper.readValue(headersJson, Map.class);
        } catch (Exception e) {
            logger.warn("Failed to deserialize headers", e);
            return null;
        }
    }
    
    public void deleteExchange(String exchangeName) {
        String sql = "DELETE FROM exchanges WHERE name = ?";
        
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, exchangeName);
            stmt.executeUpdate();
            logger.debug("Deleted exchange: {}", exchangeName);
            
        } catch (SQLException e) {
            logger.error("Failed to delete exchange: {}", exchangeName, e);
        }
    }
    
    public void deleteQueue(String queueName) {
        String sql = "DELETE FROM queues WHERE name = ?";
        
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, queueName);
            stmt.executeUpdate();
            logger.debug("Deleted queue: {}", queueName);
            
        } catch (SQLException e) {
            logger.error("Failed to delete queue: {}", queueName, e);
        }
    }
    
    public void deleteBinding(String exchangeName, String queueName, String routingKey) {
        String sql = "DELETE FROM bindings WHERE exchange_name = ? AND queue_name = ? AND routing_key = ?";

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, exchangeName);
            stmt.setString(2, queueName);
            stmt.setString(3, routingKey);
            stmt.executeUpdate();
            logger.debug("Deleted binding: {} -> {} ({})", exchangeName, queueName, routingKey);

        } catch (SQLException e) {
            logger.error("Failed to delete binding: {} -> {}", exchangeName, queueName, e);
        }
    }

    /**
     * Delete a binding with vhost support.
     */
    public void deleteBinding(String vhost, String exchangeName, String queueName, String routingKey) {
        String sql = "DELETE FROM bindings WHERE vhost = ? AND exchange_name = ? AND queue_name = ? AND routing_key = ?";

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, vhost);
            stmt.setString(2, exchangeName);
            stmt.setString(3, queueName);
            stmt.setString(4, routingKey);
            stmt.executeUpdate();
            logger.debug("Deleted binding: {} -> {} ({}) in vhost: {}", exchangeName, queueName, routingKey, vhost);

        } catch (SQLException e) {
            logger.error("Failed to delete binding: {} -> {} in vhost: {}", exchangeName, queueName, vhost, e);
        }
    }

    /**
     * Delete a queue with vhost support.
     */
    public void deleteQueue(String vhost, String queueName) {
        String sql = "DELETE FROM queues WHERE vhost = ? AND name = ?";

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, vhost);
            stmt.setString(2, queueName);
            stmt.executeUpdate();
            logger.debug("Deleted queue: {} in vhost: {}", queueName, vhost);

        } catch (SQLException e) {
            logger.error("Failed to delete queue: {} in vhost: {}", queueName, vhost, e);
        }
    }

    /**
     * Delete all messages for a queue in a specific vhost.
     */
    public void deleteAllMessages(String vhost, String queueName) {
        String sql = "DELETE FROM messages WHERE vhost = ? AND queue_name = ?";

        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, vhost);
            stmt.setString(2, queueName);
            int deleted = stmt.executeUpdate();
            logger.debug("Deleted {} messages from queue: {} in vhost: {}", deleted, queueName, vhost);

        } catch (SQLException e) {
            logger.error("Failed to delete messages from queue: {} in vhost: {}", queueName, vhost, e);
        }
    }
}