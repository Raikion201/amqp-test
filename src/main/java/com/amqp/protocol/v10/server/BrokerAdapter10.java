package com.amqp.protocol.v10.server;

import com.amqp.model.Exchange;
import com.amqp.model.Message;
import com.amqp.model.Queue;
import com.amqp.protocol.v10.connection.*;
import com.amqp.protocol.v10.delivery.Accepted;
import com.amqp.protocol.v10.messaging.*;
import com.amqp.protocol.v10.transport.*;
import com.amqp.server.AmqpBroker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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

    public BrokerAdapter10(AmqpBroker broker, Amqp10Connection connection) {
        this.broker = broker;
        this.connection = connection;
    }

    /**
     * Called when a link is attached.
     */
    public void onLinkAttached(Amqp10Session session, Amqp10Link link) {
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

    private void setupSenderLink(Amqp10Session session, SenderLink link, String address) {
        // Source address is the queue to consume from
        log.debug("Setting up sender link '{}' for queue '{}'", link.getName(), address);

        // Ensure queue exists
        Queue queue = broker.getQueue(address);
        if (queue == null) {
            // Create the queue if it doesn't exist
            queue = broker.declareQueue(address, false, false, false);
        }

        linkToQueue.put(link.getName(), address);

        // Register as consumer
        registerConsumer(session, link, address);
    }

    private void setupReceiverLink(Amqp10Session session, ReceiverLink link, String address) {
        // Target address can be a queue or exchange
        log.debug("Setting up receiver link '{}' for address '{}'", link.getName(), address);

        // Check if it's an exchange or queue
        Exchange exchange = broker.getExchange(address);
        if (exchange != null) {
            linkToExchange.put(link.getName(), address);
        } else {
            // Treat as queue
            Queue queue = broker.getQueue(address);
            if (queue == null) {
                queue = broker.declareQueue(address, false, false, false);
            }
            linkToQueue.put(link.getName(), address);
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

        linkToQueue.remove(link.getName());
        linkToExchange.remove(link.getName());
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
                // Convert to AMQP 1.0 message
                Message10 message10 = convertToAmqp10Message(message);

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
}
