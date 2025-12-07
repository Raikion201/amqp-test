package com.amqp.server;

import com.amqp.model.Exchange;
import com.amqp.model.Queue;
import com.amqp.model.Message;
import com.amqp.persistence.PersistenceManager;
import com.amqp.security.AuthenticationManager;
import com.amqp.security.VirtualHost;
import com.amqp.security.User;
import com.amqp.security.Permission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.List;

public class AmqpBroker {
    private static final Logger logger = LoggerFactory.getLogger(AmqpBroker.class);

    private final PersistenceManager persistenceManager;
    private final AuthenticationManager authenticationManager;

    public AmqpBroker(PersistenceManager persistenceManager) {
        this.persistenceManager = persistenceManager;
        this.authenticationManager = new AuthenticationManager();

        logger.info("AMQP Broker initialized with authentication and virtual hosts");
    }

    @Deprecated
    private void initializeDefaultExchanges() {
        // This method is deprecated - exchanges are now managed per virtual host
    }
    
    public synchronized Exchange declareExchange(String vhostName, User user, String name,
                                               Exchange.Type type, boolean durable,
                                               boolean autoDelete, boolean internal) {
        VirtualHost vhost = authenticationManager.getVirtualHost(vhostName);
        if (vhost == null) {
            throw new IllegalArgumentException("Virtual host not found: " + vhostName);
        }

        if (!authenticationManager.authorize(user, vhostName, Permission.CONFIGURE, name)) {
            throw new SecurityException("User does not have permission to configure exchange: " + name);
        }

        Exchange existing = vhost.getExchange(name);
        if (existing != null) {
            if (!existing.getType().equals(type) ||
                existing.isDurable() != durable ||
                existing.isAutoDelete() != autoDelete ||
                existing.isInternal() != internal) {
                throw new IllegalArgumentException("Exchange already exists with different parameters");
            }
            return existing;
        }

        Exchange exchange = new Exchange(name, type, durable, autoDelete, internal);
        vhost.addExchange(exchange);

        if (durable) {
            persistenceManager.saveExchange(exchange);
        }

        logger.info("Declared exchange: {} on vhost: {}", name, vhostName);
        return exchange;
    }

    // Legacy method for backward compatibility
    @Deprecated
    public synchronized Exchange declareExchange(String name, Exchange.Type type,
                                               boolean durable, boolean autoDelete,
                                               boolean internal) {
        User guestUser = authenticationManager.getUser("guest");
        return declareExchange("/", guestUser, name, type, durable, autoDelete, internal);
    }
    
    public synchronized Queue declareQueue(String vhostName, User user, String name,
                                         boolean durable, boolean exclusive, boolean autoDelete) {
        VirtualHost vhost = authenticationManager.getVirtualHost(vhostName);
        if (vhost == null) {
            throw new IllegalArgumentException("Virtual host not found: " + vhostName);
        }

        if (name == null || name.isEmpty()) {
            name = generateQueueName();
        }

        if (!authenticationManager.authorize(user, vhostName, Permission.CONFIGURE, name)) {
            throw new SecurityException("User does not have permission to configure queue: " + name);
        }

        Queue existing = vhost.getQueue(name);
        if (existing != null) {
            if (existing.isDurable() != durable ||
                existing.isExclusive() != exclusive ||
                existing.isAutoDelete() != autoDelete) {
                throw new IllegalArgumentException("Queue already exists with different parameters");
            }
            return existing;
        }

        Queue queue = new Queue(name, durable, exclusive, autoDelete);
        vhost.addQueue(queue);

        if (durable) {
            persistenceManager.saveQueue(queue);
        }

        logger.info("Declared queue: {} on vhost: {}", name, vhostName);
        return queue;
    }

    // Legacy method for backward compatibility
    @Deprecated
    public synchronized Queue declareQueue(String name, boolean durable,
                                         boolean exclusive, boolean autoDelete) {
        User guestUser = authenticationManager.getUser("guest");
        return declareQueue("/", guestUser, name, durable, exclusive, autoDelete);
    }
    
    public void bindQueue(String vhostName, User user, String queueName,
                         String exchangeName, String routingKey) {
        VirtualHost vhost = authenticationManager.getVirtualHost(vhostName);
        if (vhost == null) {
            throw new IllegalArgumentException("Virtual host not found: " + vhostName);
        }

        if (!authenticationManager.authorize(user, vhostName, Permission.CONFIGURE, queueName)) {
            throw new SecurityException("User does not have permission to configure queue: " + queueName);
        }

        Queue queue = vhost.getQueue(queueName);
        Exchange exchange = vhost.getExchange(exchangeName);

        if (queue == null) {
            throw new IllegalArgumentException("Queue not found: " + queueName);
        }
        if (exchange == null) {
            throw new IllegalArgumentException("Exchange not found: " + exchangeName);
        }

        exchange.addBinding(routingKey, queueName);
        persistenceManager.saveBinding(exchangeName, queueName, routingKey);

        logger.info("Bound queue {} to exchange {} with routing key {} on vhost {}",
                   queueName, exchangeName, routingKey, vhostName);
    }

    // Legacy method for backward compatibility
    @Deprecated
    public void bindQueue(String queueName, String exchangeName, String routingKey) {
        User guestUser = authenticationManager.getUser("guest");
        bindQueue("/", guestUser, queueName, exchangeName, routingKey);
    }
    
    public void publishMessage(String vhostName, User user, String exchangeName,
                              String routingKey, Message message) {
        VirtualHost vhost = authenticationManager.getVirtualHost(vhostName);
        if (vhost == null) {
            throw new IllegalArgumentException("Virtual host not found: " + vhostName);
        }

        if (!authenticationManager.authorize(user, vhostName, Permission.WRITE, exchangeName)) {
            throw new SecurityException("User does not have permission to publish to exchange: " + exchangeName);
        }

        Exchange exchange = vhost.getExchange(exchangeName);
        if (exchange == null) {
            logger.warn("Exchange not found: {}", exchangeName);
            return;
        }

        List<String> targetQueues = exchange.route(routingKey);

        for (String queueName : targetQueues) {
            Queue queue = vhost.getQueue(queueName);
            if (queue != null) {
                queue.enqueue(message);
                if (queue.isDurable() && message.isPersistent()) {
                    persistenceManager.saveMessage(queueName, message);
                }
            }
        }

        logger.debug("Published message to {} queues via exchange {} on vhost {}",
                    targetQueues.size(), exchangeName, vhostName);
    }

    // Legacy method for backward compatibility
    @Deprecated
    public void publishMessage(String exchangeName, String routingKey, Message message) {
        User guestUser = authenticationManager.getUser("guest");
        publishMessage("/", guestUser, exchangeName, routingKey, message);
    }
    
    public Message consumeMessage(String vhostName, User user, String queueName) {
        VirtualHost vhost = authenticationManager.getVirtualHost(vhostName);
        if (vhost == null) {
            throw new IllegalArgumentException("Virtual host not found: " + vhostName);
        }

        if (!authenticationManager.authorize(user, vhostName, Permission.READ, queueName)) {
            throw new SecurityException("User does not have permission to consume from queue: " + queueName);
        }

        Queue queue = vhost.getQueue(queueName);
        if (queue == null) {
            return null;
        }

        Message message = queue.dequeue();
        if (message != null && queue.isDurable() && message.isPersistent()) {
            persistenceManager.deleteMessage(message.getId());
        }

        return message;
    }

    // Legacy method for backward compatibility
    @Deprecated
    public Message consumeMessage(String queueName) {
        User guestUser = authenticationManager.getUser("guest");
        return consumeMessage("/", guestUser, queueName);
    }

    public Exchange getExchange(String vhostName, String name) {
        VirtualHost vhost = authenticationManager.getVirtualHost(vhostName);
        return vhost != null ? vhost.getExchange(name) : null;
    }

    // Legacy method for backward compatibility
    @Deprecated
    public Exchange getExchange(String name) {
        return getExchange("/", name);
    }

    public Queue getQueue(String vhostName, String name) {
        VirtualHost vhost = authenticationManager.getVirtualHost(vhostName);
        return vhost != null ? vhost.getQueue(name) : null;
    }

    // Legacy method for backward compatibility
    @Deprecated
    public Queue getQueue(String name) {
        return getQueue("/", name);
    }

    public AuthenticationManager getAuthenticationManager() {
        return authenticationManager;
    }
    
    private String generateQueueName() {
        return "amq.gen-" + System.currentTimeMillis() + "-" + 
               Thread.currentThread().getId();
    }
}