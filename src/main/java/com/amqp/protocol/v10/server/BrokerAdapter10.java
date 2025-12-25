package com.amqp.protocol.v10.server;

import com.amqp.model.Exchange;
import com.amqp.model.Message;
import com.amqp.model.Queue;
import com.amqp.protocol.v10.connection.*;
import com.amqp.protocol.v10.delivery.Accepted;
import com.amqp.protocol.v10.delivery.DeliveryState;
import com.amqp.protocol.v10.frame.Amqp10Frame;
import com.amqp.protocol.v10.frame.FrameType;
import com.amqp.protocol.v10.messaging.*;
import com.amqp.protocol.v10.transaction.Coordinator;
import com.amqp.protocol.v10.transaction.TransactionalState;
import com.amqp.protocol.v10.transport.*;
import com.amqp.server.AmqpBroker;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import com.amqp.protocol.v10.types.Symbol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Broker Adapter for AMQP 1.0.
 *
 * Adapts AMQP 1.0 link operations to the existing broker infrastructure
 * (exchanges, queues, bindings).
 */
public class BrokerAdapter10 {

    private static final Logger log = LoggerFactory.getLogger(BrokerAdapter10.class);

    private final AmqpBroker broker;
    private final Amqp10Connection connection;

    // Link to queue/exchange mappings
    private final Map<String, String> linkToQueue = new ConcurrentHashMap<>();
    private final Map<String, String> linkToExchange = new ConcurrentHashMap<>();

    // Consumer tracking - SenderLinks that send messages to clients (consuming from queues)
    private final Map<String, SenderLink> activeConsumers = new ConcurrentHashMap<>();

    // Transaction tracking - messages staged for transactional commit
    private final Map<String, List<StagedMessage>> stagedMessages = new ConcurrentHashMap<>();
    private final Map<String, List<StagedAck>> stagedAcks = new ConcurrentHashMap<>();

    // Dynamic link tracking - auto-generated queue names
    private final Map<String, String> dynamicQueues = new ConcurrentHashMap<>();
    private static final String DYNAMIC_QUEUE_PREFIX = "amq.gen-";
    private static final java.util.concurrent.atomic.AtomicLong dynamicQueueCounter =
            new java.util.concurrent.atomic.AtomicLong(0);

    // Shared scheduler for delayed message delivery - prevents thread leak
    private static final java.util.concurrent.ScheduledExecutorService DELAYED_DELIVERY_SCHEDULER =
            java.util.concurrent.Executors.newScheduledThreadPool(2,
                    r -> {
                        Thread t = new Thread(r, "amqp10-delayed-delivery");
                        t.setDaemon(true);
                        return t;
                    });

    public BrokerAdapter10(AmqpBroker broker, Amqp10Connection connection) {
        this.broker = broker;
        this.connection = connection;
    }

    // Anonymous relay links - links without a target address that route based on message "to" property
    private final Map<String, Boolean> anonymousRelayLinks = new ConcurrentHashMap<>();

    /**
     * Called when a link is attached.
     */
    public void onLinkAttached(Amqp10Session session, Amqp10Link link) {
        // Check if this is a coordinator link for transactions
        if (link instanceof CoordinatorLink) {
            setupCoordinatorLink(session, (CoordinatorLink) link);
            return;
        }

        String address = getAddress(link);

        if (address == null || address.isEmpty()) {
            // Check if this is an anonymous relay link (receiver with null target)
            if (link.isReceiver()) {
                setupAnonymousRelayLink(session, (ReceiverLink) link);
                return;
            }
            log.warn("Link {} has no address", link.getName());
            return;
        }

        if (link.isSender()) {
            // We're sending to client, so link source is a queue
            setupSenderLink(session, (SenderLink) link, address);
        } else {
            // We're receiving from client, so link target is a queue/exchange
            setupReceiverLink(session, (ReceiverLink) link, address);
        }
    }

    /**
     * Set up an anonymous relay link - messages are routed based on their "to" property.
     */
    private void setupAnonymousRelayLink(Amqp10Session session, ReceiverLink link) {
        log.info("Setting up anonymous relay link '{}'", link.getName());
        anonymousRelayLinks.put(link.getName(), true);
    }

    private void setupCoordinatorLink(Amqp10Session session, CoordinatorLink link) {
        log.info("Setting up coordinator link '{}' for transaction support", link.getName());

        // Register transaction handler
        link.setTransactionHandler(new CoordinatorLink.TransactionHandler() {
            @Override
            public void onTransactionDeclared(Transaction10 transaction) {
                String txnKey = transaction.getTxnIdString();
                stagedMessages.put(txnKey, new ArrayList<>());
                stagedAcks.put(txnKey, new ArrayList<>());
                log.debug("Transaction declared: {}", txnKey);
            }

            @Override
            public void onTransactionCommitted(Transaction10 transaction) {
                String txnKey = transaction.getTxnIdString();
                commitTransaction(txnKey);
            }

            @Override
            public void onTransactionRolledBack(Transaction10 transaction) {
                String txnKey = transaction.getTxnIdString();
                rollbackTransaction(txnKey);
            }
        });
    }

    private void setupSenderLink(Amqp10Session session, SenderLink link, String address) {
        Source source = link.getSource();
        String actualAddress = address;

        // Check for dynamic source
        if (source != null && Boolean.TRUE.equals(source.getDynamic())) {
            // Create a dynamic (temporary) queue
            actualAddress = generateDynamicQueueName();
            Queue queue = broker.declareQueue(actualAddress, false, true, true); // auto-delete, exclusive
            dynamicQueues.put(link.getName(), actualAddress);

            // Update the source with the generated address
            source.setAddress(actualAddress);
            link.setSource(source);

            log.info("Created dynamic queue '{}' for sender link '{}'", actualAddress, link.getName());
        } else {
            // Source address is the queue to consume from
            log.debug("Setting up sender link '{}' for queue '{}'", link.getName(), actualAddress);

            // Ensure queue exists
            Queue queue = broker.getQueue(actualAddress);
            if (queue == null) {
                // Create the queue if it doesn't exist
                queue = broker.declareQueue(actualAddress, false, false, false);
            }
        }

        linkToQueue.put(link.getName(), actualAddress);

        // Register as consumer
        registerConsumer(session, link, actualAddress);
    }

    private void setupReceiverLink(Amqp10Session session, ReceiverLink link, String address) {
        Target target = link.getTarget();
        String actualAddress = address;

        // Check for dynamic target
        if (target != null && Boolean.TRUE.equals(target.getDynamic())) {
            // Create a dynamic (temporary) queue
            actualAddress = generateDynamicQueueName();
            Queue queue = broker.declareQueue(actualAddress, false, true, true); // auto-delete, exclusive
            dynamicQueues.put(link.getName(), actualAddress);

            // Update the target with the generated address
            target.setAddress(actualAddress);
            link.setTarget(target);

            log.info("Created dynamic queue '{}' for receiver link '{}'", actualAddress, link.getName());
            linkToQueue.put(link.getName(), actualAddress);
        } else {
            // Target address can be a queue or exchange
            log.debug("Setting up receiver link '{}' for address '{}'", link.getName(), actualAddress);

            // Check if it's an exchange or queue
            Exchange exchange = broker.getExchange(actualAddress);
            if (exchange != null) {
                linkToExchange.put(link.getName(), actualAddress);
            } else {
                // Treat as queue
                Queue queue = broker.getQueue(actualAddress);
                if (queue == null) {
                    queue = broker.declareQueue(actualAddress, false, false, false);
                }
                linkToQueue.put(link.getName(), actualAddress);
            }
        }
    }

    private String getAddress(Amqp10Link link) {
        if (link.isSender() && link.getSource() != null) {
            return link.getSource().getAddress();
        } else if (link.isReceiver() && link.getTarget() != null) {
            return link.getTarget().getAddress();
        }
        return null;
    }

    /**
     * Called when a link is detached.
     */
    public void onLinkDetached(Amqp10Session session, Amqp10Link link) {
        log.debug("Link '{}' detached", link.getName());

        if (link.isSender()) {
            // Cancel consumer
            activeConsumers.remove(link.getName());
        }

        // Clean up dynamic queue if this link created one
        String dynamicQueue = dynamicQueues.remove(link.getName());
        if (dynamicQueue != null) {
            // Delete the dynamic queue (auto-delete queues will be cleaned up by the broker)
            try {
                broker.deleteQueue("/", null, dynamicQueue, false, false);
            } catch (Exception e) {
                log.warn("Failed to delete dynamic queue '{}': {}", dynamicQueue, e.getMessage());
            }
            log.info("Deleted dynamic queue '{}' on link '{}' detach", dynamicQueue, link.getName());
        }

        linkToQueue.remove(link.getName());
        linkToExchange.remove(link.getName());
        anonymousRelayLinks.remove(link.getName());
    }

    /**
     * Generate a unique name for a dynamic queue.
     */
    private String generateDynamicQueueName() {
        long counter = dynamicQueueCounter.incrementAndGet();
        String uuid = java.util.UUID.randomUUID().toString().substring(0, 8);
        return DYNAMIC_QUEUE_PREFIX + uuid + "-" + counter;
    }

    /**
     * Called when we receive credit from client and can send messages.
     */
    public void onCreditAvailable(Amqp10Session session, SenderLink link) {
        String queueName = linkToQueue.get(link.getName());
        if (queueName == null) {
            return;
        }

        deliverMessages(session, link, queueName);
    }

    /**
     * Called when a message is received from client.
     */
    public void onMessageReceived(Amqp10Session session, ReceiverLink link, Transfer transfer) {
        String linkName = link.getName();

        try {
            // Decode the message
            Message10 message10 = null;
            if (transfer.getPayload() != null && transfer.getPayload().isReadable()) {
                message10 = Message10.decode(transfer.getPayload().duplicate());
            }

            if (message10 == null) {
                log.warn("Failed to decode message on link {}", linkName);
                return;
            }

            // Convert to internal message format
            Message message = convertToInternalMessage(message10);

            // Check for scheduled delivery annotations
            long scheduledDelay = getScheduledDelay(message10);
            if (scheduledDelay > 0) {
                // Schedule the message for later delivery
                scheduleMessageDelivery(linkName, message10, message, scheduledDelay);
                // Send disposition with Accepted
                if (!transfer.isSettled()) {
                    sendDisposition(session, link, transfer.getDeliveryId(), Accepted.INSTANCE, true);
                }
                return;
            }

            // Check if this is an anonymous relay link
            if (anonymousRelayLinks.containsKey(linkName)) {
                // Route based on message "to" property
                String toAddress = getMessageToAddress(message10);
                if (toAddress != null && !toAddress.isEmpty()) {
                    routeMessageToAddress(toAddress, message, message10);
                } else {
                    log.warn("Anonymous relay message has no 'to' address, dropping");
                }
            } else {
                // Route the message normally
                String exchangeName = linkToExchange.get(linkName);
                if (exchangeName != null) {
                    // Publish to exchange
                    String routingKey = getRoutingKey(message10);
                    publishToExchange(exchangeName, routingKey, message);
                } else {
                    // Publish directly to queue
                    String queueName = linkToQueue.get(linkName);
                    if (queueName != null) {
                        publishToQueue(queueName, message);
                    }
                }
            }

            // Auto-accept if pre-settled or receiver-settle-mode is first
            if (!transfer.isSettled()) {
                // Send disposition with Accepted
                sendDisposition(session, link, transfer.getDeliveryId(), Accepted.INSTANCE, true);
            }

        } catch (Exception e) {
            log.error("Error processing message on link {}", linkName, e);
        }
    }

    /**
     * Get the message "to" address for anonymous relay routing.
     */
    private String getMessageToAddress(Message10 message10) {
        Properties props = message10.getProperties();
        if (props != null && props.getTo() != null) {
            return props.getTo();
        }
        return null;
    }

    /**
     * Route a message to the specified address (for anonymous relay).
     */
    private void routeMessageToAddress(String address, Message message, Message10 message10) {
        // Check if it's an exchange
        Exchange exchange = broker.getExchange(address);
        if (exchange != null) {
            String routingKey = getRoutingKey(message10);
            publishToExchange(address, routingKey, message);
        } else {
            // Treat as queue - ensure it exists
            Queue queue = broker.getQueue(address);
            if (queue == null) {
                queue = broker.declareQueue(address, false, false, false);
            }
            publishToQueue(address, message);
        }
        log.debug("Routed anonymous relay message to '{}'", address);
    }

    /**
     * Get the scheduled delivery delay from message annotations.
     * Returns 0 if no scheduling is requested.
     */
    private long getScheduledDelay(Message10 message10) {
        MessageAnnotations annotations = message10.getMessageAnnotations();
        if (annotations == null) {
            return 0;
        }

        Map<Symbol, Object> values = annotations.getValue();
        if (values == null) {
            return 0;
        }

        // Check for x-opt-delivery-time (absolute timestamp)
        Object deliveryTime = values.get(Symbol.valueOf("x-opt-delivery-time"));
        if (deliveryTime instanceof Number) {
            long scheduledTime = ((Number) deliveryTime).longValue();
            long delay = scheduledTime - System.currentTimeMillis();
            if (delay > 0) {
                return delay;
            }
            // Already past, deliver immediately
            return 0;
        }

        // Check for x-opt-delivery-delay (relative delay in milliseconds)
        Object deliveryDelay = values.get(Symbol.valueOf("x-opt-delivery-delay"));
        if (deliveryDelay instanceof Number) {
            long delay = ((Number) deliveryDelay).longValue();
            return delay > 0 ? delay : 0;
        }

        return 0;
    }

    /**
     * Schedule a message for delayed delivery.
     */
    private void scheduleMessageDelivery(String linkName, Message10 message10, Message message, long delayMs) {
        String queueName = linkToQueue.get(linkName);
        String exchangeName = linkToExchange.get(linkName);

        // Check for anonymous relay
        if (anonymousRelayLinks.containsKey(linkName)) {
            queueName = getMessageToAddress(message10);
        }

        final String targetQueue = queueName;
        final String targetExchange = exchangeName;
        final String routingKey = getRoutingKey(message10);

        log.info("Scheduling message delivery in {}ms to {}", delayMs,
                targetQueue != null ? targetQueue : targetExchange);

        // Use the shared scheduler to delay delivery (prevents thread leak)
        DELAYED_DELIVERY_SCHEDULER.schedule(() -> {
            try {
                if (targetExchange != null) {
                    publishToExchange(targetExchange, routingKey, message);
                } else if (targetQueue != null) {
                    // Ensure queue exists
                    Queue queue = broker.getQueue(targetQueue);
                    if (queue == null) {
                        broker.declareQueue(targetQueue, false, false, false);
                    }
                    publishToQueue(targetQueue, message);
                }
                log.debug("Delivered scheduled message to {}",
                        targetQueue != null ? targetQueue : targetExchange);
            } catch (Exception e) {
                log.error("Error delivering scheduled message", e);
            }
        }, delayMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private Message convertToInternalMessage(Message10 message10) {
        Message message = new Message();

        // Extract body and preserve body type
        byte[] body = message10.getBodyAsBytes();
        if (body != null) {
            message.setBody(body);
        } else {
            String bodyStr = message10.getBodyAsString();
            if (bodyStr != null) {
                message.setBody(bodyStr.getBytes());
            }
        }

        // Preserve AMQP 1.0 body type
        if (message10.isDataBody()) {
            message.setBodyType(Message.BodyType.DATA);
        } else if (message10.isAmqpValueBody()) {
            message.setBodyType(Message.BodyType.AMQP_VALUE);
        } else if (message10.isAmqpSequenceBody()) {
            message.setBodyType(Message.BodyType.AMQP_SEQUENCE);
        }

        // Copy properties
        Properties props = message10.getProperties();
        if (props != null) {
            if (props.getMessageId() != null) {
                message.setMessageId(props.getMessageIdAsString());
            }
            if (props.getCorrelationId() != null) {
                message.setCorrelationId(props.getCorrelationIdAsString());
            }
            if (props.getContentType() != null) {
                message.setContentType(props.getContentTypeAsString());
            }
            if (props.getReplyTo() != null) {
                message.setReplyTo(props.getReplyTo());
            }
            if (props.getSubject() != null) {
                message.setSubject(props.getSubject());
            }
        }

        // Copy header settings
        Header header = message10.getHeader();
        if (header != null) {
            log.debug("Message has Header: durable={}, priority={}, ttl={}",
                    header.getDurable(), header.getPriority(), header.getTtl());
            message.setDeliveryMode((short) (header.isDurable() ? 2 : 1));
            if (header.getPriority() != null) {
                message.setPriority(header.getPriority().shortValue());
            }
            if (header.getTtl() != null) {
                // Set TTL for proper expiry checking
                message.setTtl(header.getTtl());
                long expiration = System.currentTimeMillis() + header.getTtl();
                message.setExpiration(String.valueOf(expiration));
                log.debug("Set message TTL to {}ms, expiration={}", header.getTtl(), expiration);
            }
        } else {
            log.debug("Message has no Header section");
        }

        // Extract absolute expiry time from Properties
        if (props != null && props.getAbsoluteExpiryTime() != null) {
            long absExpiryTime = props.getAbsoluteExpiryTime().getTime();
            message.setAbsoluteExpiryTime(absExpiryTime);
            log.debug("Set message absolute expiry time to {}", absExpiryTime);
        }

        // Copy application properties
        ApplicationProperties appProps = message10.getApplicationProperties();
        if (appProps != null && !appProps.isEmpty()) {
            Map<String, Object> headers = new HashMap<>();
            for (Map.Entry<String, Object> entry : appProps.getValue().entrySet()) {
                headers.put(entry.getKey(), entry.getValue());
            }
            message.setHeaders(headers);
        }

        return message;
    }

    private String getRoutingKey(Message10 message10) {
        // Try to get routing key from properties or annotations
        Properties props = message10.getProperties();
        if (props != null && props.getSubject() != null) {
            return props.getSubject();
        }

        ApplicationProperties appProps = message10.getApplicationProperties();
        if (appProps != null) {
            Object rk = appProps.get("x-routing-key");
            if (rk != null) {
                return rk.toString();
            }
        }

        return "";
    }

    private void publishToExchange(String exchangeName, String routingKey, Message message) {
        Exchange exchange = broker.getExchange(exchangeName);
        if (exchange != null) {
            // Route through the exchange and deliver to matching queues
            java.util.List<String> targetQueues = exchange.route(routingKey, message);
            for (String queueName : targetQueues) {
                Queue queue = broker.getQueue(queueName);
                if (queue != null) {
                    queue.enqueue(message);
                }
            }
            log.debug("Published message to exchange '{}' with routing key '{}', delivered to {} queues",
                    exchangeName, routingKey, targetQueues.size());
        }
    }

    private void publishToQueue(String queueName, Message message) {
        Queue queue = broker.getQueue(queueName);
        if (queue != null) {
            queue.enqueue(message);
            log.debug("Published message to queue '{}', queue@{}, size after enqueue: {}",
                    queueName, System.identityHashCode(queue), queue.size());
        } else {
            log.warn("Queue '{}' not found for publishing", queueName);
        }
    }

    private void registerConsumer(Amqp10Session session, SenderLink link, String queueName) {
        // Store reference for message delivery
        activeConsumers.put(link.getName(), link);

        // Start delivering messages when credit is available
        log.debug("Registered consumer on queue '{}' for link '{}'", queueName, link.getName());
    }

    private void deliverMessages(Amqp10Session session, SenderLink link, String queueName) {
        Queue queue = broker.getQueue(queueName);
        if (queue == null) {
            log.debug("deliverMessages: Queue '{}' not found", queueName);
            return;
        }

        log.debug("deliverMessages: Queue '{}' queue@{} has {} messages, link hasCredit={}, isAttached={}",
                queueName, System.identityHashCode(queue), queue.size(), link.hasCredit(), link.isAttached());

        while (link.hasCredit() && link.isAttached()) {
            Message message = queue.dequeue();
            if (message == null) {
                log.debug("deliverMessages: No more messages in queue '{}'", queueName);
                break;
            }
            log.debug("deliverMessages: Dequeued message from '{}', expiration={}", queueName, message.getExpiration());

            try {
                // Check TTL - if message has expired, skip it
                if (isMessageExpired(message)) {
                    log.debug("Skipping expired message on queue '{}'", queueName);
                    // Handle dead-letter if configured
                    handleExpiredMessage(message, queueName);
                    continue;
                }

                // Convert to AMQP 1.0 message
                Message10 message10 = convertToAmqp10Message(message);

                // Update TTL for remaining time
                updateRemainingTtl(message10, message);

                // Send via link
                SenderLink.SenderDelivery delivery = link.send(message10, false);
                if (delivery == null) {
                    // No credit, put message back
                    queue.enqueue(message);
                    break;
                }

                // Track queue name and internal message for requeue on Released
                delivery.setQueueName(queueName);
                delivery.setInternalMessage(message);
                log.debug("Set delivery tracking: deliveryId={}, queueName={}, message={}",
                        delivery.getDeliveryId(), queueName, message.getMessageId());

                // Actually send the Transfer frame over the wire
                sendTransfer(session, delivery.getTransfer());
                log.debug("Delivered message to link '{}'", link.getName());

            } catch (Exception e) {
                log.error("Error delivering message on link {}", link.getName(), e);
                // Put message back on queue
                queue.enqueue(message);
                break;
            }
        }
    }

    /**
     * Check if a message has expired based on TTL, absolute expiry time, or string expiration.
     */
    private boolean isMessageExpired(Message message) {
        // Use the comprehensive isExpired() method from Message
        return message.isExpired();
    }

    /**
     * Handle an expired message (dead-letter if configured).
     */
    private void handleExpiredMessage(Message message, String queueName) {
        Queue queue = broker.getQueue(queueName);
        if (queue != null) {
            // Check for dead-letter exchange configuration
            Map<String, Object> args = queue.getQueueArguments();
            if (args != null) {
                Object dlx = args.get("x-dead-letter-exchange");
                if (dlx != null) {
                    String dlxName = dlx.toString();
                    Object dlrk = args.get("x-dead-letter-routing-key");
                    String routingKey = dlrk != null ? dlrk.toString() : "";

                    // Add death info
                    Map<String, Object> headers = message.getHeaders();
                    if (headers == null) {
                        headers = new HashMap<>();
                    }
                    headers.put("x-death-reason", "expired");
                    headers.put("x-death-queue", queueName);
                    headers.put("x-death-time", System.currentTimeMillis());
                    message.setHeaders(headers);

                    // Route to DLX
                    publishToExchange(dlxName, routingKey, message);
                    log.debug("Expired message routed to dead-letter exchange '{}' from queue '{}'",
                            dlxName, queueName);
                    return;
                }
            }
        }
        // If no DLX, message is simply discarded
        log.debug("Expired message discarded from queue '{}' (no DLX configured)", queueName);
    }

    /**
     * Update the TTL in the AMQP 1.0 message to reflect remaining time.
     */
    private void updateRemainingTtl(Message10 message10, Message original) {
        String expiration = original.getExpiration();
        if (expiration == null || expiration.isEmpty()) {
            return;
        }

        try {
            long expirationTime = Long.parseLong(expiration);
            if (expirationTime > 0) {
                long remainingTtl = expirationTime - System.currentTimeMillis();
                if (remainingTtl > 0) {
                    Header header = message10.getHeader();
                    if (header == null) {
                        header = new Header();
                        message10.setHeader(header);
                    }
                    header.setTtl(remainingTtl);
                }
            }
        } catch (NumberFormatException e) {
            // Ignore invalid expiration
        }
    }

    private Message10 convertToAmqp10Message(Message message) {
        Message10 message10 = new Message10();

        // Set body - preserve the original body type
        byte[] body = message.getBody();
        if (body != null) {
            Message.BodyType bodyType = message.getBodyType();
            if (bodyType == Message.BodyType.AMQP_VALUE) {
                // Convert bytes back to string for AmqpValue
                String text = new String(body, java.nio.charset.StandardCharsets.UTF_8);
                message10.setBody(new AmqpValue(text));
            } else if (bodyType == Message.BodyType.AMQP_SEQUENCE) {
                // AmqpSequence - just use Data for now as sequence conversion is complex
                message10.setBody(new Data(body));
            } else {
                // Default to Data (preserves binary content)
                message10.setBody(new Data(body));
            }
        }

        // Set header
        Header header = new Header();
        header.setDurable(message.getDeliveryMode() == 2);
        if (message.getPriority() > 0) {
            header.setPriority((int) message.getPriority());
        }
        message10.setHeader(header);

        // Set properties
        Properties props = new Properties();
        if (message.getMessageId() != null) {
            props.setMessageId(message.getMessageId());
        }
        if (message.getCorrelationId() != null) {
            props.setCorrelationId(message.getCorrelationId());
        }
        if (message.getContentType() != null) {
            props.setContentType(message.getContentType());
        }
        if (message.getReplyTo() != null) {
            props.setReplyTo(message.getReplyTo());
        }
        if (message.getSubject() != null) {
            props.setSubject(message.getSubject());
        }
        // Preserve absolute expiry time
        if (message.getAbsoluteExpiryTime() != null && message.getAbsoluteExpiryTime() > 0) {
            props.setAbsoluteExpiryTime(new java.util.Date(message.getAbsoluteExpiryTime()));
        }
        message10.setProperties(props);

        // Set application properties from headers
        Map<String, Object> headers = message.getHeaders();
        if (headers != null && !headers.isEmpty()) {
            ApplicationProperties appProps = new ApplicationProperties(headers);
            message10.setApplicationProperties(appProps);
        }

        return message10;
    }

    private void sendDisposition(Amqp10Session session, ReceiverLink link,
                                  long deliveryId, Object state, boolean settled) {
        Disposition disposition = link.createDisposition(deliveryId,
                state instanceof com.amqp.protocol.v10.delivery.DeliveryState
                        ? (com.amqp.protocol.v10.delivery.DeliveryState) state
                        : Accepted.INSTANCE,
                settled);

        // Send the disposition frame
        sendPerformative(session, disposition);
    }

    /**
     * Send a Transfer frame over the wire.
     * Handles multi-frame transfers for large messages.
     */
    private void sendTransfer(Amqp10Session session, Transfer transfer) {
        Channel channel = connection.getChannel();
        ByteBuf payload = transfer.getPayload();

        // Get the negotiated max frame size
        int maxFrameSize = connection.getMaxFrameSize();

        // Calculate overhead for frame header (8 bytes) and estimated performative size
        // Frame header: 4 (size) + 1 (doff) + 1 (type) + 2 (channel) = 8 bytes
        // Performative overhead: ~50 bytes for Transfer with typical fields
        int frameOverhead = 8 + 64; // Conservative estimate
        int maxPayloadPerFrame = maxFrameSize - frameOverhead;

        if (maxPayloadPerFrame < 256) {
            maxPayloadPerFrame = 256; // Minimum reasonable payload size
        }

        if (payload == null || !payload.isReadable() || payload.readableBytes() <= maxPayloadPerFrame) {
            // Small message - send in single frame
            ByteBuf body = channel.alloc().buffer();
            transfer.encode(body);

            if (payload != null && payload.isReadable()) {
                body.writeBytes(payload);
            }

            Amqp10Frame frame = new Amqp10Frame(FrameType.AMQP, session.getLocalChannel(), body);
            channel.writeAndFlush(frame);
            log.trace("Sent Transfer on channel {} (single frame)", session.getLocalChannel());
        } else {
            // Large message - split into multiple frames
            int totalBytes = payload.readableBytes();
            int offset = 0;
            int frameCount = 0;

            log.debug("Splitting large message ({} bytes) into multiple frames (max {} bytes per frame)",
                    totalBytes, maxPayloadPerFrame);

            while (offset < totalBytes) {
                int remaining = totalBytes - offset;
                int chunkSize = Math.min(remaining, maxPayloadPerFrame);
                boolean hasMore = (offset + chunkSize) < totalBytes;

                // Create a new Transfer for this chunk
                Transfer chunkTransfer = new Transfer(transfer.getHandle());

                // Only first frame has delivery-id and delivery-tag
                if (offset == 0) {
                    chunkTransfer.setDeliveryId(transfer.getDeliveryId());
                    chunkTransfer.setDeliveryTag(transfer.getDeliveryTag());
                    chunkTransfer.setMessageFormat(transfer.getMessageFormat());
                    chunkTransfer.setSettled(transfer.getSettled());
                }

                // Set more flag (true if there are more frames coming)
                chunkTransfer.setMore(hasMore);

                // Encode frame
                ByteBuf body = channel.alloc().buffer();
                chunkTransfer.encode(body);

                // Write chunk of payload
                body.writeBytes(payload, offset, chunkSize);

                Amqp10Frame frame = new Amqp10Frame(FrameType.AMQP, session.getLocalChannel(), body);
                channel.writeAndFlush(frame);

                offset += chunkSize;
                frameCount++;
            }

            log.debug("Sent Transfer on channel {} ({} frames for {} bytes)",
                    session.getLocalChannel(), frameCount, totalBytes);
        }
    }

    /**
     * Send a performative frame over the wire.
     */
    private void sendPerformative(Amqp10Session session, Performative performative) {
        Channel channel = connection.getChannel();
        ByteBuf body = channel.alloc().buffer();

        performative.encode(body);

        Amqp10Frame frame = new Amqp10Frame(FrameType.AMQP, session.getLocalChannel(), body);
        channel.writeAndFlush(frame);

        log.trace("Sent {} on channel {}", performative.getClass().getSimpleName(), session.getLocalChannel());
    }

    /**
     * Handle disposition received from the client for a sender link.
     * This is called when the client sends Accepted, Released, Rejected, etc.
     */
    public void onSenderDisposition(Amqp10Session session, SenderLink link,
                                     SenderLink.SenderDelivery delivery, Object state) {
        log.debug("onSenderDisposition: link={}, deliveryId={}, state={}, stateClass={}",
                link.getName(), delivery.getDeliveryId(), state,
                state != null ? state.getClass().getName() : "null");

        // Check if Released - need to requeue message
        if (isReleasedState(state)) {
            String queueName = delivery.getQueueName();
            Object internalMsg = delivery.getInternalMessage();

            log.debug("Released state detected: queueName={}, internalMsg={}",
                    queueName, internalMsg != null ? internalMsg.getClass().getName() : "null");

            if (queueName != null && internalMsg instanceof Message) {
                Message message = (Message) internalMsg;
                Queue queue = broker.getQueue(queueName);
                if (queue != null) {
                    queue.enqueue(message);
                    log.info("Released message requeued to queue '{}'", queueName);
                } else {
                    log.warn("Cannot requeue Released message - queue '{}' not found", queueName);
                }
            } else {
                log.debug("Released disposition but no queue/message info available: queueName={}, internalMsgType={}",
                        queueName, internalMsg != null ? internalMsg.getClass().getName() : "null");
            }
        }
        // For Accepted, just log - message is already removed from queue
        else if (isAcceptedState(state)) {
            log.debug("Message accepted by receiver on link '{}'", link.getName());
        }
        // For Rejected, we could dead-letter the message, but for now just log
        else if (isRejectedState(state)) {
            log.debug("Message rejected by receiver on link '{}', discarding", link.getName());
        }
    }

    private boolean isReleasedState(Object state) {
        if (state == null) return false;
        if (state instanceof com.amqp.protocol.v10.delivery.Released) return true;
        // Check for DescribedType with Released descriptor (0x26)
        if (state instanceof com.amqp.protocol.v10.types.DescribedType) {
            com.amqp.protocol.v10.types.DescribedType described =
                (com.amqp.protocol.v10.types.DescribedType) state;
            Object descriptor = described.getDescriptor();
            if (descriptor instanceof Number) {
                return ((Number) descriptor).longValue() == 0x0000000000000026L;
            }
        }
        // Check class name as fallback
        return state.getClass().getSimpleName().equals("Released");
    }

    private boolean isAcceptedState(Object state) {
        if (state == null) return false;
        if (state instanceof com.amqp.protocol.v10.delivery.Accepted) return true;
        if (state instanceof Accepted) return true;
        return state.getClass().getSimpleName().equals("Accepted");
    }

    private boolean isRejectedState(Object state) {
        if (state == null) return false;
        return state.getClass().getSimpleName().equals("Rejected");
    }

    // Transaction support methods

    /**
     * Stage a message for transactional publish.
     */
    public void stageMessage(String txnKey, String address, Message message, boolean isExchange) {
        stageMessage(txnKey, address, message, isExchange, null);
    }

    /**
     * Stage a message for transactional publish with routing key.
     */
    public void stageMessage(String txnKey, String address, Message message,
                              boolean isExchange, String routingKey) {
        List<StagedMessage> staged = stagedMessages.get(txnKey);
        if (staged != null) {
            staged.add(new StagedMessage(address, message, isExchange, routingKey));
            log.debug("Staged message for transaction {} to {} (routingKey: {})",
                      txnKey, address, routingKey);
        } else {
            log.warn("Unknown transaction: {}", txnKey);
        }
    }

    /**
     * Stage an acknowledgment for transactional ack.
     */
    public void stageAck(String txnKey, String queueName, Message message) {
        List<StagedAck> staged = stagedAcks.get(txnKey);
        if (staged != null) {
            staged.add(new StagedAck(queueName, message));
            log.debug("Staged ack for transaction {} on queue {}", txnKey, queueName);
        } else {
            log.warn("Unknown transaction for ack: {}", txnKey);
        }
    }

    /**
     * Commit a transaction - make all staged operations permanent.
     */
    private void commitTransaction(String txnKey) {
        log.info("Committing transaction: {}", txnKey);

        // Commit staged messages (publish them)
        List<StagedMessage> messages = stagedMessages.remove(txnKey);
        if (messages != null) {
            for (StagedMessage staged : messages) {
                if (staged.isExchange) {
                    String routingKey = staged.routingKey != null ? staged.routingKey : "";
                    publishToExchange(staged.address, routingKey, staged.message);
                } else {
                    publishToQueue(staged.address, staged.message);
                }
            }
            log.debug("Committed {} messages for transaction {}", messages.size(), txnKey);
        }

        // Commit staged acks (finalize message removals)
        List<StagedAck> acks = stagedAcks.remove(txnKey);
        if (acks != null) {
            // Messages are already removed from queue on dequeue
            // Nothing more to do for acks on commit
            log.debug("Committed {} acks for transaction {}", acks.size(), txnKey);
        }
    }

    /**
     * Rollback a transaction - discard all staged operations.
     */
    private void rollbackTransaction(String txnKey) {
        log.info("Rolling back transaction: {}", txnKey);

        // Discard staged messages
        List<StagedMessage> messages = stagedMessages.remove(txnKey);
        if (messages != null) {
            log.debug("Discarded {} staged messages for transaction {}", messages.size(), txnKey);
        }

        // Rollback staged acks (re-enqueue messages)
        List<StagedAck> acks = stagedAcks.remove(txnKey);
        if (acks != null) {
            for (StagedAck staged : acks) {
                Queue queue = broker.getQueue(staged.queueName);
                if (queue != null) {
                    queue.enqueue(staged.message);
                }
            }
            log.debug("Re-enqueued {} messages for transaction rollback {}", acks.size(), txnKey);
        }
    }

    /**
     * Handle a transactional message received from client.
     */
    public void onTransactionalMessageReceived(Amqp10Session session, ReceiverLink link,
                                                Transfer transfer, byte[] txnId) {
        String linkName = link.getName();
        String txnKey = bytesToHex(txnId);

        try {
            // Decode the message
            Message10 message10 = null;
            if (transfer.getPayload() != null && transfer.getPayload().isReadable()) {
                message10 = Message10.decode(transfer.getPayload().duplicate());
            }

            if (message10 == null) {
                log.warn("Failed to decode transactional message on link {}", linkName);
                return;
            }

            // Convert to internal message format
            Message message = convertToInternalMessage(message10);

            // Extract routing key from message
            String routingKey = getRoutingKey(message10);

            // Stage for transactional commit
            String exchangeName = linkToExchange.get(linkName);
            if (exchangeName != null) {
                stageMessage(txnKey, exchangeName, message, true, routingKey);
            } else {
                String queueName = linkToQueue.get(linkName);
                if (queueName != null) {
                    stageMessage(txnKey, queueName, message, false, null);
                }
            }

            // Send transactional disposition (not settled yet)
            if (!transfer.isSettled()) {
                TransactionalState txnState = new TransactionalState(txnId, Accepted.INSTANCE);
                sendDisposition(session, link, transfer.getDeliveryId(), txnState, false);
            }

        } catch (Exception e) {
            log.error("Error processing transactional message on link {}", linkName, e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * Staged message for transactional publish.
     */
    private static class StagedMessage {
        final String address;
        final Message message;
        final boolean isExchange;
        final String routingKey;

        StagedMessage(String address, Message message, boolean isExchange) {
            this(address, message, isExchange, null);
        }

        StagedMessage(String address, Message message, boolean isExchange, String routingKey) {
            this.address = address;
            this.message = message;
            this.isExchange = isExchange;
            this.routingKey = routingKey;
        }
    }

    /**
     * Staged acknowledgment for transactional ack.
     */
    private static class StagedAck {
        final String queueName;
        final Message message;

        StagedAck(String queueName, Message message) {
            this.queueName = queueName;
            this.message = message;
        }
    }
}
