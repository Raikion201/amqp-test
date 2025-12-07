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
    
    public void saveExchange(Exchange exchange) {
        String sql = """
            INSERT INTO exchanges (name, type, durable, auto_delete, internal)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (name) DO NOTHING
            """;
        
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, exchange.getName());
            stmt.setString(2, exchange.getType().name());
            stmt.setBoolean(3, exchange.isDurable());
            stmt.setBoolean(4, exchange.isAutoDelete());
            stmt.setBoolean(5, exchange.isInternal());
            
            stmt.executeUpdate();
            logger.debug("Saved exchange: {}", exchange.getName());
            
        } catch (SQLException e) {
            logger.error("Failed to save exchange: {}", exchange.getName(), e);
        }
    }
    
    public void saveQueue(Queue queue) {
        String sql = """
            INSERT INTO queues (name, durable, exclusive, auto_delete)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (name) DO NOTHING
            """;
        
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, queue.getName());
            stmt.setBoolean(2, queue.isDurable());
            stmt.setBoolean(3, queue.isExclusive());
            stmt.setBoolean(4, queue.isAutoDelete());
            
            stmt.executeUpdate();
            logger.debug("Saved queue: {}", queue.getName());
            
        } catch (SQLException e) {
            logger.error("Failed to save queue: {}", queue.getName(), e);
        }
    }
    
    public void saveBinding(String exchangeName, String queueName, String routingKey) {
        String sql = """
            INSERT INTO bindings (exchange_name, queue_name, routing_key)
            VALUES (?, ?, ?)
            ON CONFLICT (exchange_name, queue_name, routing_key) DO NOTHING
            """;
        
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, exchangeName);
            stmt.setString(2, queueName);
            stmt.setString(3, routingKey);
            
            stmt.executeUpdate();
            logger.debug("Saved binding: {} -> {} ({})", exchangeName, queueName, routingKey);
            
        } catch (SQLException e) {
            logger.error("Failed to save binding: {} -> {}", exchangeName, queueName, e);
        }
    }
    
    public void saveMessage(String queueName, Message message) {
        String sql = """
            INSERT INTO messages (queue_name, routing_key, content_type, content_encoding,
                                headers, delivery_mode, priority, correlation_id, reply_to,
                                expiration, message_id, timestamp, type, user_id, app_id,
                                cluster_id, body)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, queueName);
            stmt.setString(2, message.getRoutingKey());
            stmt.setString(3, message.getContentType());
            stmt.setString(4, message.getContentEncoding());
            stmt.setString(5, serializeHeaders(message.getHeaders()));
            stmt.setShort(6, message.getDeliveryMode());
            stmt.setShort(7, message.getPriority());
            stmt.setString(8, message.getCorrelationId());
            stmt.setString(9, message.getReplyTo());
            stmt.setString(10, message.getExpiration());
            stmt.setString(11, message.getMessageId());
            stmt.setLong(12, message.getTimestamp());
            stmt.setString(13, message.getType());
            stmt.setString(14, message.getUserId());
            stmt.setString(15, message.getAppId());
            stmt.setString(16, message.getClusterId());
            stmt.setBytes(17, message.getBody());
            
            stmt.executeUpdate();
            logger.debug("Saved message to queue: {}", queueName);
            
        } catch (SQLException e) {
            logger.error("Failed to save message to queue: {}", queueName, e);
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
}