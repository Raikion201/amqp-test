package com.amqp.server;

import com.amqp.model.Exchange;
import com.amqp.model.Queue;
import com.amqp.model.Message;
import com.amqp.persistence.PersistenceManager;
import com.amqp.security.AuthenticationManager;
import com.amqp.security.VirtualHost;
import com.amqp.security.User;
import com.amqp.security.Permission;
import com.amqp.consumer.ConsumerManager;
import com.amqp.consumer.MessageDeliveryService;
import com.amqp.connection.AmqpConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.List;
import java.util.UUID;

public class AmqpBroker {
    private static final Logger logger = LoggerFactory.getLogger(AmqpBroker.class);

    private final PersistenceManager persistenceManager;
    private final AuthenticationManager authenticationManager;
    private final ConsumerManager consumerManager;
    private final MessageDeliveryService deliveryService;
    private final AtomicLong queueNameCounter = new AtomicLong(0);
    private final ScheduledExecutorService heartbeatScheduler;

    public AmqpBroker(PersistenceManager persistenceManager) {
        this(persistenceManager, false); // Default: guest user DISABLED
    }

    public AmqpBroker(PersistenceManager persistenceManager, boolean enableGuestUser) {
        this.persistenceManager = persistenceManager;
        this.authenticationManager = new AuthenticationManager(enableGuestUser);
        this.consumerManager = new ConsumerManager();
        this.deliveryService = new MessageDeliveryService(consumerManager);
        this.heartbeatScheduler = Executors.newScheduledThreadPool(
            2, // Small pool for heartbeat tasks
            r -> {
                Thread t = new Thread(r, "Heartbeat-Scheduler");
                t.setDaemon(true);
                return t;
            }
        );

        logger.info("AMQP Broker initialized with authentication, virtual hosts, and message delivery");
    }

    public synchronized Exchange declareExchange(String vhostName, User user, String name,
                                               Exchange.Type type, boolean durable,
                                               boolean autoDelete, boolean internal) {
        // Validate exchange name (allow empty for default exchange)
        if (name != null && !name.isEmpty()) {
            validateResourceName(name, "Exchange");
        }

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
            persistenceManager.saveExchange(vhostName, exchange);
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
        } else {
            // Validate queue name
            validateResourceName(name, "Queue");
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
            persistenceManager.saveQueue(vhostName, queue);
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
        persistenceManager.saveBinding(vhostName, exchangeName, queueName, routingKey);

        logger.info("Bound queue {} to exchange {} with routing key {} on vhost {}",
                   queueName, exchangeName, routingKey, vhostName);
    }

    // Legacy method for backward compatibility
    @Deprecated
    public void bindQueue(String queueName, String exchangeName, String routingKey) {
        User guestUser = authenticationManager.getUser("guest");
        bindQueue("/", guestUser, queueName, exchangeName, routingKey);
    }

    public void unbindQueue(String vhostName, User user, String queueName,
                           String exchangeName, String routingKey) {
        VirtualHost vhost = authenticationManager.getVirtualHost(vhostName);
        if (vhost == null) {
            throw new IllegalArgumentException("Virtual host not found: " + vhostName);
        }

        if (!authenticationManager.authorize(user, vhostName, Permission.CONFIGURE, queueName)) {
            throw new SecurityException("User does not have permission to configure queue: " + queueName);
        }

        Exchange exchange = vhost.getExchange(exchangeName);
        if (exchange == null) {
            throw new IllegalArgumentException("Exchange not found: " + exchangeName);
        }

        exchange.removeBinding(routingKey, queueName);
        persistenceManager.deleteBinding(vhostName, exchangeName, queueName, routingKey);

        logger.info("Unbound queue {} from exchange {} with routing key {} on vhost {}",
                   queueName, exchangeName, routingKey, vhostName);
    }

    public int purgeQueue(String vhostName, User user, String queueName) {
        VirtualHost vhost = authenticationManager.getVirtualHost(vhostName);
        if (vhost == null) {
            throw new IllegalArgumentException("Virtual host not found: " + vhostName);
        }

        if (!authenticationManager.authorize(user, vhostName, Permission.WRITE, queueName)) {
            throw new SecurityException("User does not have permission to purge queue: " + queueName);
        }

        Queue queue = vhost.getQueue(queueName);
        if (queue == null) {
            throw new IllegalArgumentException("Queue not found: " + queueName);
        }

        int purgedCount = queue.size();
        queue.clear();

        // Also delete persisted messages for this queue
        persistenceManager.deleteAllMessages(vhostName, queueName);

        logger.info("Purged {} messages from queue {} on vhost {}", purgedCount, queueName, vhostName);
        return purgedCount;
    }

    public int deleteQueue(String vhostName, User user, String queueName,
                          boolean ifUnused, boolean ifEmpty) {
        VirtualHost vhost = authenticationManager.getVirtualHost(vhostName);
        if (vhost == null) {
            throw new IllegalArgumentException("Virtual host not found: " + vhostName);
        }

        if (!authenticationManager.authorize(user, vhostName, Permission.CONFIGURE, queueName)) {
            throw new SecurityException("User does not have permission to delete queue: " + queueName);
        }

        Queue queue = vhost.getQueue(queueName);
        if (queue == null) {
            return 0; // Queue doesn't exist, nothing to delete
        }

        // Check conditions
        if (ifUnused && consumerManager.hasConsumersForQueue(queueName)) {
            throw new IllegalStateException("Queue is in use: " + queueName);
        }

        if (ifEmpty && queue.size() > 0) {
            throw new IllegalStateException("Queue is not empty: " + queueName);
        }

        int messageCount = queue.size();

        // Remove from all exchange bindings
        for (Exchange exchange : vhost.getAllExchanges()) {
            exchange.removeAllBindingsToQueue(queueName);
        }

        // Remove consumers
        consumerManager.removeConsumersForQueue(queueName);

        // Remove the queue from vhost
        vhost.removeQueue(queueName);

        // Delete persisted queue and messages
        persistenceManager.deleteQueue(vhostName, queueName);
        persistenceManager.deleteAllMessages(vhostName, queueName);

        logger.info("Deleted queue {} with {} messages on vhost {}", queueName, messageCount, vhostName);
        return messageCount;
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

        List<String> targetQueues;

        // Special handling for default exchange (empty name)
        // The default exchange routes directly to queue with name = routing key
        if (exchangeName == null || exchangeName.isEmpty()) {
            targetQueues = new java.util.ArrayList<>();
            // Check if queue exists with name = routing key
            Queue directQueue = vhost.getQueue(routingKey);
            if (directQueue != null) {
                targetQueues.add(routingKey);
            } else {
                logger.debug("Default exchange: no queue found with name '{}'", routingKey);
            }
        } else {
            Exchange exchange = vhost.getExchange(exchangeName);
            if (exchange == null) {
                logger.warn("Exchange not found: {}", exchangeName);
                return;
            }
            targetQueues = exchange.route(routingKey, message);
        }

        for (String queueName : targetQueues) {
            Queue queue = vhost.getQueue(queueName);
            if (queue != null) {
                queue.enqueue(message);
                if (queue.isDurable() && message.isPersistent()) {
                    persistenceManager.saveMessage(vhostName, queueName, message);
                }
                // Trigger immediate delivery check for this queue
                deliveryService.triggerDelivery(queueName);
            }
        }

        logger.debug("Published message to {} queues via exchange {} on vhost {}",
                    targetQueues.size(), exchangeName, vhostName);

        // Process dead-lettered messages from all target queues
        for (String queueName : targetQueues) {
            processDeadLetterMessages(vhostName, queueName);
        }
    }

    /**
     * Process dead-lettered messages from a queue and forward them to the DLX exchange.
     */
    public void processDeadLetterMessages(String vhostName, String queueName) {
        VirtualHost vhost = authenticationManager.getVirtualHost(vhostName);
        if (vhost == null) {
            return;
        }

        Queue queue = vhost.getQueue(queueName);
        if (queue == null || !(queue instanceof com.amqp.model.EnhancedQueue)) {
            return;
        }

        com.amqp.model.EnhancedQueue enhancedQueue = (com.amqp.model.EnhancedQueue) queue;
        com.amqp.model.QueueArguments args = enhancedQueue.getArguments();

        if (!args.hasDLX()) {
            return; // No DLX configured
        }

        // Get and clear dead letter queue
        java.util.List<com.amqp.model.EnhancedQueue.MessageWrapper> deadLetters =
            enhancedQueue.getAndClearDeadLetterQueue();

        if (deadLetters.isEmpty()) {
            return;
        }

        String dlxExchange = args.getDeadLetterExchange();
        String dlxRoutingKey = args.getDeadLetterRoutingKey();

        logger.debug("Processing {} dead-lettered messages from queue {} to DLX {}",
                    deadLetters.size(), queueName, dlxExchange);

        // Publish each dead-lettered message to the DLX
        User systemUser = authenticationManager.getUser("guest"); // Use system user for DLX
        for (com.amqp.model.EnhancedQueue.MessageWrapper wrapper : deadLetters) {
            Message dlxMessage = wrapper.getMessage();

            // Use DLX routing key if configured, otherwise use original routing key
            String routingKey = dlxRoutingKey != null ? dlxRoutingKey : dlxMessage.getRoutingKey();
            if (routingKey == null) {
                routingKey = "";
            }

            try {
                // Publish to DLX exchange
                publishMessage(vhostName, systemUser, dlxExchange, routingKey, dlxMessage);
                logger.debug("Dead-lettered message forwarded to DLX: exchange={}, routingKey={}",
                            dlxExchange, routingKey);
            } catch (Exception e) {
                logger.error("Failed to forward dead-lettered message to DLX: {}", dlxExchange, e);
            }
        }
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

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public MessageDeliveryService getDeliveryService() {
        return deliveryService;
    }

    /**
     * Start the message delivery service.
     * Must be called after broker initialization.
     */
    public void start() {
        recoverDurableResources();
        deliveryService.start();
        logger.info("AMQP Broker started");
    }

    /**
     * Recover durable exchanges, queues, bindings, and messages from persistence on startup.
     */
    private void recoverDurableResources() {
        logger.info("Starting recovery of durable resources...");

        // Recover for all virtual hosts
        for (VirtualHost vhost : authenticationManager.getAllVirtualHosts()) {
            String vhostName = vhost.getName();
            logger.info("Recovering resources for vhost: {}", vhostName);

            try {
                // 1. Recover durable exchanges
                List<PersistenceManager.ExchangeData> exchanges = persistenceManager.loadExchanges(vhostName);
                for (PersistenceManager.ExchangeData exchangeData : exchanges) {
                    // Skip if already exists (e.g., default exchanges)
                    if (vhost.getExchange(exchangeData.name) != null) {
                        logger.debug("Exchange {} already exists in vhost {}, skipping recovery",
                                   exchangeData.name, vhostName);
                        continue;
                    }

                    Exchange.Type type = Exchange.Type.valueOf(exchangeData.type);
                    Exchange exchange = new Exchange(
                        exchangeData.name,
                        type,
                        exchangeData.durable,
                        exchangeData.autoDelete,
                        exchangeData.internal
                    );
                    vhost.addExchange(exchange);
                    logger.debug("Recovered exchange: {} in vhost: {}", exchangeData.name, vhostName);
                }

                // 2. Recover durable queues
                List<PersistenceManager.QueueData> queues = persistenceManager.loadQueues(vhostName);
                for (PersistenceManager.QueueData queueData : queues) {
                    // Skip if already exists
                    if (vhost.getQueue(queueData.name) != null) {
                        logger.debug("Queue {} already exists in vhost {}, skipping recovery",
                                   queueData.name, vhostName);
                        continue;
                    }

                    Queue queue = new Queue(
                        queueData.name,
                        queueData.durable,
                        queueData.exclusive,
                        queueData.autoDelete
                    );
                    vhost.addQueue(queue);
                    logger.debug("Recovered queue: {} in vhost: {}", queueData.name, vhostName);
                }

                // 3. Recover bindings
                List<PersistenceManager.BindingData> bindings = persistenceManager.loadBindings(vhostName);
                for (PersistenceManager.BindingData bindingData : bindings) {
                    Exchange exchange = vhost.getExchange(bindingData.exchangeName);
                    if (exchange != null) {
                        exchange.addBinding(bindingData.routingKey, bindingData.queueName);
                        logger.debug("Recovered binding: {} -> {} ({}) in vhost: {}",
                                   bindingData.exchangeName, bindingData.queueName,
                                   bindingData.routingKey, vhostName);
                    } else {
                        logger.warn("Cannot recover binding: exchange {} not found in vhost: {}",
                                  bindingData.exchangeName, vhostName);
                    }
                }

                // 4. Recover persisted messages for each queue
                for (PersistenceManager.QueueData queueData : queues) {
                    Queue queue = vhost.getQueue(queueData.name);
                    if (queue != null) {
                        List<Message> messages = persistenceManager.loadMessages(vhostName, queueData.name);
                        for (Message message : messages) {
                            queue.enqueue(message);
                        }
                        logger.debug("Recovered {} messages for queue: {} in vhost: {}",
                                   messages.size(), queueData.name, vhostName);
                    }
                }

                logger.info("Recovery completed for vhost: {} - {} exchanges, {} queues, {} bindings",
                          vhostName, exchanges.size(), queues.size(), bindings.size());

            } catch (Exception e) {
                logger.error("Failed to recover resources for vhost: {}", vhostName, e);
            }
        }

        logger.info("Durable resource recovery completed");
    }

    /**
     * Stop the message delivery service and cleanup all resources.
     * Should be called on broker shutdown.
     */
    public void stop() {
        deliveryService.stop();

        // Shutdown heartbeat scheduler
        if (heartbeatScheduler != null && !heartbeatScheduler.isShutdown()) {
            heartbeatScheduler.shutdown();
            try {
                if (!heartbeatScheduler.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    heartbeatScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                heartbeatScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Cleanup all QuorumQueue schedulers
        for (VirtualHost vhost : authenticationManager.getAllVirtualHosts()) {
            for (Queue queue : vhost.getAllQueues()) {
                if (queue instanceof com.amqp.queue.QuorumQueue) {
                    ((com.amqp.queue.QuorumQueue) queue).shutdown();
                }
            }
        }

        logger.info("AMQP Broker stopped");
    }

    /**
     * Register a consumer for a queue and start message delivery.
     */
    public ConsumerManager.Consumer addConsumer(String vhostName, String queueName, String consumerTag,
                                                short channelNumber, boolean noAck, boolean exclusive,
                                                java.util.Map<String, Object> arguments,
                                                AmqpConnection connection) {
        Queue queue = getQueue(vhostName, queueName);
        if (queue == null) {
            throw new IllegalArgumentException("Queue not found: " + queueName + " in vhost: " + vhostName);
        }

        ConsumerManager.Consumer consumer = consumerManager.addConsumer(
            consumerTag, queueName, channelNumber, noAck, exclusive, arguments, connection
        );

        // Start delivering messages to this queue
        deliveryService.startDelivery(queue);

        logger.info("Consumer added: {} for queue: {} in vhost: {}", consumerTag, queueName, vhostName);
        return consumer;
    }

    /**
     * Cancel a consumer.
     */
    public void cancelConsumer(String consumerTag) {
        ConsumerManager.Consumer consumer = consumerManager.removeConsumer(consumerTag);
        if (consumer != null) {
            String queueName = consumer.getQueueName();
            // If no more consumers for this queue, stop delivery
            if (consumerManager.getConsumerCountForQueue(queueName) == 0) {
                deliveryService.stopDelivery(queueName);
            }
            logger.info("Consumer cancelled: {}", consumerTag);
        }
    }

    /**
     * Acknowledge a message delivery - delete from persistence if durable.
     */
    public void acknowledgeMessage(String vhostName, String queueName, Message message) {
        VirtualHost vhost = authenticationManager.getVirtualHost(vhostName);
        if (vhost == null) {
            logger.warn("Cannot acknowledge message: vhost not found: {}", vhostName);
            return;
        }

        Queue queue = vhost.getQueue(queueName);
        if (queue != null && queue.isDurable() && message.isPersistent() && message.getId() > 0) {
            persistenceManager.deleteMessage(message.getId());
            logger.debug("Deleted persisted message: id={}, queue={}", message.getId(), queueName);
        }
    }

    /**
     * Requeue a message that was rejected or nacked.
     */
    public void requeueMessage(String vhostName, String queueName, Message message) {
        VirtualHost vhost = authenticationManager.getVirtualHost(vhostName);
        if (vhost == null) {
            logger.warn("Cannot requeue message: vhost not found: {}", vhostName);
            return;
        }

        Queue queue = vhost.getQueue(queueName);
        if (queue != null) {
            queue.enqueue(message);
            logger.debug("Requeued message to queue: {}", queueName);
        } else {
            logger.warn("Cannot requeue message: queue not found: {}", queueName);
        }
    }

    public PersistenceManager getPersistenceManager() {
        return persistenceManager;
    }

    public ScheduledExecutorService getHeartbeatScheduler() {
        return heartbeatScheduler;
    }

    private String generateQueueName() {
        // Use UUID for guaranteed uniqueness across all threads and time
        return "amq.gen-" + UUID.randomUUID().toString();
    }

    /**
     * Validate resource names (queues, exchanges) for security and correctness.
     * Prevents injection attacks and ensures AMQP compliance.
     */
    private void validateResourceName(String name, String resourceType) {
        if (name == null) {
            throw new IllegalArgumentException(resourceType + " name cannot be null");
        }

        if (name.isEmpty()) {
            throw new IllegalArgumentException(resourceType + " name cannot be empty");
        }

        // Check maximum length (AMQP spec: short string max 255 bytes)
        if (name.length() > 255) {
            throw new IllegalArgumentException(resourceType + " name exceeds maximum length of 255 characters");
        }

        // Validate characters: alphanumeric, dash, underscore, period, colon only
        // Note: Allow 'amq.' prefix for server-generated names
        if (!name.matches("^[a-zA-Z0-9._:\\-]+$")) {
            throw new IllegalArgumentException(resourceType + " name contains invalid characters. " +
                                             "Only alphanumeric, dash, underscore, period, and colon are allowed");
        }

        // Reserve 'amq.' prefix for server use (unless it's a generated name)
        if (name.startsWith("amq.") && !name.startsWith("amq.gen-")) {
            throw new IllegalArgumentException(resourceType + " name cannot start with reserved prefix 'amq.' " +
                                             "(reserved for server-generated resources)");
        }
    }
}