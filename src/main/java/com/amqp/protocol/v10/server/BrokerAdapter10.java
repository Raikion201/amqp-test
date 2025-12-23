package com.amqp.protocol.v10.server;

import com.amqp.model.Exchange;
import com.amqp.model.Message;
import com.amqp.model.Queue;
import com.amqp.protocol.v10.connection.*;
import com.amqp.protocol.v10.delivery.Accepted;
import com.amqp.protocol.v10.delivery.DeliveryState;
import com.amqp.protocol.v10.messaging.*;
import com.amqp.protocol.v10.transaction.Coordinator;
import com.amqp.protocol.v10.transaction.TransactionalState;
import com.amqp.protocol.v10.transport.*;
import com.amqp.server.AmqpBroker;
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

    // Consumer tracking
    private final Map<String, ReceiverLink> activeConsumers = new ConcurrentHashMap<>();

    // Transaction tracking - messages staged for transactional commit
    private final Map<String, List<StagedMessage>> stagedMessages = new ConcurrentHashMap<>();
    private final Map<String, List<StagedAck>> stagedAcks = new ConcurrentHashMap<>();

    // Dynamic link tracking - auto-generated queue names
    private final Map<String, String> dynamicQueues = new ConcurrentHashMap<>();
    private static final String DYNAMIC_QUEUE_PREFIX = "amq.gen-";
    private static final java.util.concurrent.atomic.AtomicLong dynamicQueueCounter =
            new java.util.concurrent.atomic.AtomicLong(0);

    public BrokerAdapter10(AmqpBroker broker, Amqp10Connection connection) {
        this.broker = broker;
        this.connection = connection;
    }

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

            // Route the message
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

            // Auto-accept if pre-settled or receiver-settle-mode is first
            if (!transfer.isSettled()) {
                // Send disposition with Accepted
                sendDisposition(session, link, transfer.getDeliveryId(), Accepted.INSTANCE, true);
            }

        } catch (Exception e) {
            log.error("Error processing message on link {}", linkName, e);
        }
    }

    private Message convertToInternalMessage(Message10 message10) {
        Message message = new Message();

        // Extract body
        byte[] body = message10.getBodyAsBytes();
        if (body != null) {
            message.setBody(body);
        } else {
            String bodyStr = message10.getBodyAsString();
            if (bodyStr != null) {
                message.setBody(bodyStr.getBytes());
            }
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
        }

        // Copy header settings
        Header header = message10.getHeader();
        if (header != null) {
            message.setDeliveryMode((short) (header.isDurable() ? 2 : 1));
            if (header.getPriority() != null) {
                message.setPriority(header.getPriority().shortValue());
            }
            if (header.getTtl() != null) {
                message.setExpiration(String.valueOf(System.currentTimeMillis() + header.getTtl()));
            }
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
            log.debug("Published message to queue '{}'", queueName);
        }
    }

    private void registerConsumer(Amqp10Session session, SenderLink link, String queueName) {
        // Store reference for message delivery
        activeConsumers.put(link.getName(), null); // Placeholder

        // Start delivering messages when credit is available
        log.debug("Registered consumer on queue '{}' for link '{}'", queueName, link.getName());
    }

    private void deliverMessages(Amqp10Session session, SenderLink link, String queueName) {
        Queue queue = broker.getQueue(queueName);
        if (queue == null) {
            return;
        }

        while (link.hasCredit() && link.isAttached()) {
            Message message = queue.dequeue();
            if (message == null) {
                break;
            }

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
     * Check if a message has expired based on its TTL.
     */
    private boolean isMessageExpired(Message message) {
        String expiration = message.getExpiration();
        if (expiration == null || expiration.isEmpty()) {
            return false;
        }

        try {
            long expirationTime = Long.parseLong(expiration);
            return Header.isExpiredAt(expirationTime);
        } catch (NumberFormatException e) {
            return false;
        }
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

        // Set body
        byte[] body = message.getBody();
        if (body != null) {
            message10.setBody(new Data(body));
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

        // The handler will send this
        // For now we rely on the connection handler to send it
    }

    // Transaction support methods

    /**
     * Stage a message for transactional publish.
     */
    public void stageMessage(String txnKey, String address, Message message, boolean isExchange) {
        List<StagedMessage> staged = stagedMessages.get(txnKey);
        if (staged != null) {
            staged.add(new StagedMessage(address, message, isExchange));
            log.debug("Staged message for transaction {} to {}", txnKey, address);
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
                    String routingKey = ""; // TODO: Extract from message
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

            // Stage for transactional commit
            String exchangeName = linkToExchange.get(linkName);
            if (exchangeName != null) {
                stageMessage(txnKey, exchangeName, message, true);
            } else {
                String queueName = linkToQueue.get(linkName);
                if (queueName != null) {
                    stageMessage(txnKey, queueName, message, false);
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

        StagedMessage(String address, Message message, boolean isExchange) {
            this.address = address;
            this.message = message;
            this.isExchange = isExchange;
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
