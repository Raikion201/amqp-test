package com.amqp.connection;

import com.amqp.amqp.AmqpFrame;
import com.amqp.amqp.AmqpMethod;
import com.amqp.amqp.AmqpCodec;
import com.amqp.amqp.AmqpConstants;
import com.amqp.model.Message;
import com.amqp.model.Queue;
import com.amqp.model.Exchange;
import com.amqp.server.AmqpBroker;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class AmqpChannel {
    private static final Logger logger = LoggerFactory.getLogger(AmqpChannel.class);

    private final short channelNumber;
    private final AmqpConnection connection;
    private final AmqpBroker broker;
    private final AtomicBoolean open;
    private final ConcurrentMap<String, String> consumers;
    private volatile int prefetchCount = 0;
    private volatile boolean inTransaction = false;

    // Transaction buffering - synchronized on transactionLock
    private final Object transactionLock = new Object();
    private final List<TransactionAction> transactionBuffer = new ArrayList<>();

    // Acknowledgement tracking
    private final AtomicLong deliveryTagGenerator = new AtomicLong(0);
    private final ConcurrentMap<Long, UnackedMessage> unackedMessages = new ConcurrentHashMap<>();

    // Multi-frame message assembly state - synchronized on publishLock
    private final Object publishLock = new Object();
    private PublishContext currentPublishContext;

    /**
     * Represents an unacknowledged message that has been delivered to a consumer.
     */
    private static class UnackedMessage {
        final Message message;
        final String queueName;
        final String vhost;
        final boolean redelivered;

        UnackedMessage(Message message, String queueName, String vhost, boolean redelivered) {
            this.message = message;
            this.queueName = queueName;
            this.vhost = vhost;
            this.redelivered = redelivered;
        }
    }

    private static class PublishContext {
        String exchangeName;
        String routingKey;
        boolean mandatory;
        boolean immediate;
        Message message;
        long expectedBodySize;
        ByteBuf bodyBuffer;

        PublishContext(String exchangeName, String routingKey, boolean mandatory, boolean immediate) {
            this.exchangeName = exchangeName;
            this.routingKey = routingKey;
            this.mandatory = mandatory;
            this.immediate = immediate;
            this.message = new Message();
            this.message.setRoutingKey(routingKey);
        }

        void releaseBuffer() {
            if (bodyBuffer != null) {
                bodyBuffer.release();
                bodyBuffer = null;
            }
        }
    }

    /**
     * Represents an action that is buffered during a transaction.
     */
    private interface TransactionAction {
        void execute();
    }

    /**
     * Publish action buffered during a transaction.
     */
    private class PublishAction implements TransactionAction {
        final String vhost;
        final com.amqp.security.User user;
        final String exchangeName;
        final String routingKey;
        final Message message;

        PublishAction(String vhost, com.amqp.security.User user, String exchangeName,
                     String routingKey, Message message) {
            this.vhost = vhost;
            this.user = user;
            this.exchangeName = exchangeName;
            this.routingKey = routingKey;
            this.message = message;
        }

        @Override
        public void execute() {
            broker.publishMessage(vhost, user, exchangeName, routingKey, message);
        }
    }

    /**
     * Acknowledgement action buffered during a transaction.
     */
    private class AckAction implements TransactionAction {
        final long deliveryTag;
        final boolean multiple;

        AckAction(long deliveryTag, boolean multiple) {
            this.deliveryTag = deliveryTag;
            this.multiple = multiple;
        }

        @Override
        public void execute() {
            acknowledgeMessage(deliveryTag, multiple);
        }
    }

    /**
     * Reject action buffered during a transaction.
     */
    private class RejectAction implements TransactionAction {
        final long deliveryTag;
        final boolean requeue;

        RejectAction(long deliveryTag, boolean requeue) {
            this.deliveryTag = deliveryTag;
            this.requeue = requeue;
        }

        @Override
        public void execute() {
            rejectMessage(deliveryTag, requeue);
        }
    }
    
    public AmqpChannel(short channelNumber, AmqpConnection connection, AmqpBroker broker) {
        this.channelNumber = channelNumber;
        this.connection = connection;
        this.broker = broker;
        this.open = new AtomicBoolean(true);
        this.consumers = new ConcurrentHashMap<>();
    }
    
    public void handleFrame(AmqpFrame frame) {
        if (!open.get()) {
            logger.warn("Received frame on closed channel: {}", channelNumber);
            return;
        }
        
        switch (AmqpFrame.FrameType.fromValue(frame.getType())) {
            case METHOD:
                handleMethodFrame(frame);
                break;
            case HEADER:
                handleHeaderFrame(frame);
                break;
            case BODY:
                handleBodyFrame(frame);
                break;
            case HEARTBEAT:
                handleHeartbeat(frame);
                break;
        }
    }
    
    private void handleMethodFrame(AmqpFrame frame) {
        ByteBuf payload = frame.getPayload();
        short classId = payload.readShort();
        short methodId = payload.readShort();
        
        logger.debug("Handling method: class={}, method={}, channel={}", classId, methodId, channelNumber);
        
        switch (classId) {
            case AmqpConstants.CLASS_CONNECTION:
                handleConnectionMethod(methodId, payload);
                break;
            case AmqpConstants.CLASS_CHANNEL:
                handleChannelMethod(methodId, payload);
                break;
            case AmqpConstants.CLASS_EXCHANGE:
                handleExchangeMethod(methodId, payload);
                break;
            case AmqpConstants.CLASS_QUEUE:
                handleQueueMethod(methodId, payload);
                break;
            case AmqpConstants.CLASS_BASIC:
                handleBasicMethod(methodId, payload);
                break;
            case AmqpConstants.CLASS_TX:
                handleTxMethod(methodId, payload);
                break;
            default:
                logger.warn("Unknown class ID: {}", classId);
        }
    }
    
    private void handleConnectionMethod(short methodId, ByteBuf payload) {
        switch (methodId) {
            case AmqpConstants.METHOD_CONNECTION_START_OK:
                handleConnectionStartOk(payload);
                break;
            case AmqpConstants.METHOD_CONNECTION_TUNE_OK:
                handleConnectionTuneOk(payload);
                break;
            case AmqpConstants.METHOD_CONNECTION_OPEN:
                handleConnectionOpen(payload);
                break;
            case AmqpConstants.METHOD_CONNECTION_CLOSE:
                handleConnectionClose(payload);
                break;
        }
    }
    
    private void handleChannelMethod(short methodId, ByteBuf payload) {
        switch (methodId) {
            case AmqpConstants.METHOD_CHANNEL_OPEN:
                handleChannelOpen(payload);
                break;
            case AmqpConstants.METHOD_CHANNEL_CLOSE:
                handleChannelClose(payload);
                break;
        }
    }
    
    private void handleExchangeMethod(short methodId, ByteBuf payload) {
        switch (methodId) {
            case AmqpConstants.METHOD_EXCHANGE_DECLARE:
                handleExchangeDeclare(payload);
                break;
            case AmqpConstants.METHOD_EXCHANGE_DELETE:
                handleExchangeDelete(payload);
                break;
        }
    }
    
    private void handleQueueMethod(short methodId, ByteBuf payload) {
        switch (methodId) {
            case AmqpConstants.METHOD_QUEUE_DECLARE:
                handleQueueDeclare(payload);
                break;
            case AmqpConstants.METHOD_QUEUE_BIND:
                handleQueueBind(payload);
                break;
            case AmqpConstants.METHOD_QUEUE_UNBIND:
                handleQueueUnbind(payload);
                break;
            case AmqpConstants.METHOD_QUEUE_PURGE:
                handleQueuePurge(payload);
                break;
            case AmqpConstants.METHOD_QUEUE_DELETE:
                handleQueueDelete(payload);
                break;
        }
    }

    private void handleTxMethod(short methodId, ByteBuf payload) {
        switch (methodId) {
            case AmqpConstants.METHOD_TX_SELECT:
                handleTxSelect(payload);
                break;
            case AmqpConstants.METHOD_TX_COMMIT:
                handleTxCommit(payload);
                break;
            case AmqpConstants.METHOD_TX_ROLLBACK:
                handleTxRollback(payload);
                break;
            default:
                logger.warn("Unknown TX method: {}", methodId);
        }
    }

    private void handleTxSelect(ByteBuf payload) {
        logger.debug("Tx Select on channel {}", channelNumber);
        txSelect();
        // Send Tx.Select-Ok
        connection.sendMethod(channelNumber, AmqpConstants.CLASS_TX, AmqpConstants.METHOD_TX_SELECT_OK, null);
    }

    private void handleTxCommit(ByteBuf payload) {
        logger.debug("Tx Commit on channel {}", channelNumber);
        txCommit();
        // Send Tx.Commit-Ok
        connection.sendMethod(channelNumber, AmqpConstants.CLASS_TX, AmqpConstants.METHOD_TX_COMMIT_OK, null);
    }

    private void handleTxRollback(ByteBuf payload) {
        logger.debug("Tx Rollback on channel {}", channelNumber);
        txRollback();
        // Send Tx.Rollback-Ok
        connection.sendMethod(channelNumber, AmqpConstants.CLASS_TX, AmqpConstants.METHOD_TX_ROLLBACK_OK, null);
    }
    
    private void handleBasicMethod(short methodId, ByteBuf payload) {
        switch (methodId) {
            case AmqpConstants.METHOD_BASIC_QOS:
                handleBasicQos(payload);
                break;
            case AmqpConstants.METHOD_BASIC_PUBLISH:
                handleBasicPublish(payload);
                break;
            case AmqpConstants.METHOD_BASIC_CONSUME:
                handleBasicConsume(payload);
                break;
            case AmqpConstants.METHOD_BASIC_CANCEL:
                handleBasicCancel(payload);
                break;
            case AmqpConstants.METHOD_BASIC_GET:
                handleBasicGet(payload);
                break;
            case AmqpConstants.METHOD_BASIC_ACK:
                handleBasicAck(payload);
                break;
            case AmqpConstants.METHOD_BASIC_REJECT:
                handleBasicReject(payload);
                break;
            case AmqpConstants.METHOD_BASIC_NACK:
                handleBasicNack(payload);
                break;
            default:
                logger.warn("Unknown Basic method: {}", methodId);
        }
    }

    private void handleBasicQos(ByteBuf payload) {
        int prefetchSize = payload.readInt(); // prefetch-size (ignored by RabbitMQ)
        short prefetchCount = payload.readShort(); // prefetch-count
        boolean global = AmqpCodec.decodeBoolean(payload);

        logger.debug("Basic QoS: prefetchSize={}, prefetchCount={}, global={}",
                    prefetchSize, prefetchCount, global);

        setQoS(prefetchCount);

        // Send Qos-Ok
        connection.sendMethod(channelNumber, AmqpConstants.CLASS_BASIC, AmqpConstants.METHOD_BASIC_QOS_OK, null);
    }

    private void handleBasicCancel(ByteBuf payload) {
        String consumerTag = AmqpCodec.decodeShortString(payload);
        boolean noWait = AmqpCodec.decodeBoolean(payload);

        logger.debug("Basic Cancel: consumerTag={}, noWait={}", consumerTag, noWait);

        try {
            // Remove consumer from broker
            broker.cancelConsumer(consumerTag);
            removeConsumer(consumerTag);

            if (!noWait) {
                // Send Cancel-Ok
                ByteBuf cancelOkPayload = Unpooled.buffer();
                AmqpCodec.encodeShortString(cancelOkPayload, consumerTag);
                connection.sendMethod(channelNumber, AmqpConstants.CLASS_BASIC,
                                     AmqpConstants.METHOD_BASIC_CANCEL_OK, cancelOkPayload);
            }
        } catch (Exception e) {
            logger.error("Failed to cancel consumer: {}", consumerTag, e);
        }
    }
    
    private void handleConnectionStartOk(ByteBuf payload) {
        // client-properties is a table: 4-byte length + table data
        int tableLength = payload.readInt();
        if (tableLength > 0) {
            payload.skipBytes(tableLength); // Skip the table data
        }

        // mechanism is a short-string
        String mechanism = AmqpCodec.decodeShortString(payload);
        // response is a long-string (SASL response)
        String response = AmqpCodec.decodeLongString(payload);
        // locale is a short-string
        String locale = AmqpCodec.decodeShortString(payload);

        logger.debug("Connection Start-Ok: mechanism={}, locale={}", mechanism, locale);

        // Authenticate user
        if ("PLAIN".equals(mechanism)) {
            try {
                // Parse SASL PLAIN response: \0username\0password
                byte[] responseBytes = response.getBytes(java.nio.charset.StandardCharsets.UTF_8);

                int firstNull = -1;
                int secondNull = -1;

                for (int i = 0; i < responseBytes.length; i++) {
                    if (responseBytes[i] == 0) {
                        if (firstNull == -1) {
                            firstNull = i;
                        } else {
                            secondNull = i;
                            break;
                        }
                    }
                }

                if (firstNull == -1 || secondNull == -1) {
                    sendAuthenticationError("Invalid SASL PLAIN response format");
                    return;
                }

                String username = new String(responseBytes, firstNull + 1, secondNull - firstNull - 1,
                                            java.nio.charset.StandardCharsets.UTF_8);
                String password = new String(responseBytes, secondNull + 1, responseBytes.length - secondNull - 1,
                                            java.nio.charset.StandardCharsets.UTF_8);

                logger.debug("Authenticating user: {}", username);

                // Authenticate with broker
                com.amqp.security.User user = broker.getAuthenticationManager().authenticate(username, password);

                if (user == null) {
                    logger.warn("Authentication failed for user: {}", username);
                    sendAuthenticationError("ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN");
                    return;
                }

                // Authentication successful
                connection.setUser(user);
                logger.info("User authenticated successfully: {}", username);

                // Send Connection.Tune
                ByteBuf tunePayload = Unpooled.buffer();
                tunePayload.writeShort(AmqpConstants.DEFAULT_CHANNEL_MAX);
                tunePayload.writeInt(AmqpConstants.DEFAULT_FRAME_MAX);
                tunePayload.writeShort(AmqpConstants.DEFAULT_HEARTBEAT);

                connection.sendMethod(channelNumber, AmqpConstants.CLASS_CONNECTION,
                                    AmqpConstants.METHOD_CONNECTION_TUNE, tunePayload);

            } catch (Exception e) {
                logger.error("Error during authentication", e);
                sendAuthenticationError("Internal authentication error");
            }
        } else {
            sendAuthenticationError("Unsupported authentication mechanism: " + mechanism);
        }
    }

    private void sendAuthenticationError(String message) {
        logger.warn("Sending authentication error: {}", message);

        // Send Connection.Close with ACCESS_REFUSED
        ByteBuf closePayload = Unpooled.buffer();
        closePayload.writeShort(AmqpConstants.REPLY_ACCESS_REFUSED);
        AmqpCodec.encodeShortString(closePayload, message);
        closePayload.writeShort(AmqpConstants.CLASS_CONNECTION);
        closePayload.writeShort(AmqpConstants.METHOD_CONNECTION_START_OK);

        connection.sendMethod(channelNumber, AmqpConstants.CLASS_CONNECTION,
                            AmqpConstants.METHOD_CONNECTION_CLOSE, closePayload);

        // Close the connection after a short delay
        connection.close();
    }
    
    private void handleConnectionTuneOk(ByteBuf payload) {
        short channelMax = payload.readShort();
        int frameMax = payload.readInt();
        short heartbeat = payload.readShort();

        logger.debug("Connection Tune-Ok: channelMax={}, frameMax={}, heartbeat={}",
                    channelMax, frameMax, heartbeat);

        // Start heartbeat monitoring if enabled
        if (heartbeat > 0 && broker.getHeartbeatScheduler() != null) {
            connection.startHeartbeat(heartbeat, broker.getHeartbeatScheduler());
        }
    }
    
    private void handleConnectionOpen(ByteBuf payload) {
        String virtualHost = AmqpCodec.decodeShortString(payload);
        payload.skipBytes(2);
        
        logger.debug("Connection Open: virtualHost={}", virtualHost);
        connection.setVirtualHost(virtualHost);
        connection.setConnected(true);
        
        ByteBuf openOkPayload = Unpooled.buffer();
        AmqpCodec.encodeShortString(openOkPayload, "");
        
        connection.sendMethod(channelNumber, (short) 10, (short) 41, openOkPayload);
    }
    
    private void handleConnectionClose(ByteBuf payload) {
        short replyCode = payload.readShort();
        String replyText = AmqpCodec.decodeShortString(payload);
        
        logger.info("Connection Close: code={}, text={}", replyCode, replyText);
        
        connection.sendMethod(channelNumber, (short) 10, (short) 51, null);
        connection.close();
    }
    
    private void handleChannelOpen(ByteBuf payload) {
        logger.debug("Channel Open: {}", channelNumber);
        
        ByteBuf openOkPayload = Unpooled.buffer();
        AmqpCodec.encodeLongString(openOkPayload, "");
        
        connection.sendMethod(channelNumber, (short) 20, (short) 11, openOkPayload);
    }
    
    private void handleChannelClose(ByteBuf payload) {
        short replyCode = payload.readShort();
        String replyText = AmqpCodec.decodeShortString(payload);
        
        logger.debug("Channel Close: code={}, text={}", replyCode, replyText);
        
        connection.sendMethod(channelNumber, (short) 20, (short) 41, null);
        close();
    }
    
    private void handleExchangeDeclare(ByteBuf payload) {
        payload.skipBytes(2); // reserved
        String exchangeName = AmqpCodec.decodeShortString(payload);
        String exchangeType = AmqpCodec.decodeShortString(payload);
        // Flags are packed as bit flags in a single byte
        byte flags = payload.readByte();
        boolean passive = (flags & 0x01) != 0;
        boolean durable = (flags & 0x02) != 0;
        boolean autoDelete = (flags & 0x04) != 0;
        boolean internal = (flags & 0x08) != 0;
        boolean nowait = (flags & 0x10) != 0;
        
        logger.debug("Exchange Declare: name={}, type={}", exchangeName, exchangeType);

        try {
            // Get user from connection
            com.amqp.security.User user = connection.getUser();
            if (user == null) {
                user = broker.getAuthenticationManager().getUser("guest");
            }

            Exchange.Type type = Exchange.Type.valueOf(exchangeType.toUpperCase());
            broker.declareExchange(connection.getVirtualHost(), user, exchangeName,
                                 type, durable, autoDelete, internal);

            if (!nowait) {
                connection.sendMethod(channelNumber, (short) 40, (short) 11, null);
            }
        } catch (Exception e) {
            logger.error("Failed to declare exchange: {}", exchangeName, e);
        }
    }
    
    private void handleExchangeDelete(ByteBuf payload) {
        payload.skipBytes(2);
        String exchangeName = AmqpCodec.decodeShortString(payload);
        
        logger.debug("Exchange Delete: name={}", exchangeName);
        connection.sendMethod(channelNumber, (short) 40, (short) 21, null);
    }
    
    private void handleQueueDeclare(ByteBuf payload) {
        payload.skipBytes(2); // reserved
        String queueName = AmqpCodec.decodeShortString(payload);
        // Flags are packed as bit flags in a single byte
        byte flags = payload.readByte();
        boolean passive = (flags & 0x01) != 0;
        boolean durable = (flags & 0x02) != 0;
        boolean exclusive = (flags & 0x04) != 0;
        boolean autoDelete = (flags & 0x08) != 0;
        boolean nowait = (flags & 0x10) != 0;
        
        logger.debug("Queue Declare: name={}", queueName);

        try {
            // Get user from connection
            com.amqp.security.User user = connection.getUser();
            if (user == null) {
                user = broker.getAuthenticationManager().getUser("guest");
            }

            Queue queue = broker.declareQueue(connection.getVirtualHost(), user,
                                             queueName, durable, exclusive, autoDelete);

            if (!nowait) {
                ByteBuf declareOkPayload = Unpooled.buffer();
                AmqpCodec.encodeShortString(declareOkPayload, queue.getName());
                declareOkPayload.writeInt(queue.size());
                declareOkPayload.writeInt(0);

                connection.sendMethod(channelNumber, (short) 50, (short) 11, declareOkPayload);
            }
        } catch (Exception e) {
            logger.error("Failed to declare queue: {}", queueName, e);
        }
    }
    
    private void handleQueueBind(ByteBuf payload) {
        payload.skipBytes(2);
        String queueName = AmqpCodec.decodeShortString(payload);
        String exchangeName = AmqpCodec.decodeShortString(payload);
        String routingKey = AmqpCodec.decodeShortString(payload);
        boolean nowait = AmqpCodec.decodeBoolean(payload);
        
        logger.debug("Queue Bind: queue={}, exchange={}, routingKey={}",
                    queueName, exchangeName, routingKey);

        try {
            // Get user from connection
            com.amqp.security.User user = connection.getUser();
            if (user == null) {
                user = broker.getAuthenticationManager().getUser("guest");
            }

            broker.bindQueue(connection.getVirtualHost(), user, queueName, exchangeName, routingKey);

            if (!nowait) {
                connection.sendMethod(channelNumber, (short) 50, (short) 21, null);
            }
        } catch (Exception e) {
            logger.error("Failed to bind queue: {}", queueName, e);
        }
    }
    
    private void handleQueueUnbind(ByteBuf payload) {
        payload.skipBytes(2); // reserved
        String queueName = AmqpCodec.decodeShortString(payload);
        String exchangeName = AmqpCodec.decodeShortString(payload);
        String routingKey = AmqpCodec.decodeShortString(payload);

        logger.debug("Queue Unbind: queue={}, exchange={}, routingKey={}",
                    queueName, exchangeName, routingKey);

        try {
            com.amqp.security.User user = connection.getUser();
            if (user == null) {
                user = broker.getAuthenticationManager().getUser("guest");
            }

            broker.unbindQueue(connection.getVirtualHost(), user, queueName, exchangeName, routingKey);

            // Send Unbind-Ok
            connection.sendMethod(channelNumber, AmqpConstants.CLASS_QUEUE,
                                 AmqpConstants.METHOD_QUEUE_UNBIND_OK, null);
        } catch (Exception e) {
            logger.error("Failed to unbind queue: {}", queueName, e);
        }
    }

    private void handleQueuePurge(ByteBuf payload) {
        payload.skipBytes(2); // reserved
        String queueName = AmqpCodec.decodeShortString(payload);
        boolean noWait = AmqpCodec.decodeBoolean(payload);

        logger.debug("Queue Purge: queue={}, noWait={}", queueName, noWait);

        try {
            com.amqp.security.User user = connection.getUser();
            if (user == null) {
                user = broker.getAuthenticationManager().getUser("guest");
            }

            int purgedCount = broker.purgeQueue(connection.getVirtualHost(), user, queueName);

            if (!noWait) {
                // Send Purge-Ok with message count
                ByteBuf purgeOkPayload = Unpooled.buffer();
                purgeOkPayload.writeInt(purgedCount);
                connection.sendMethod(channelNumber, AmqpConstants.CLASS_QUEUE,
                                     AmqpConstants.METHOD_QUEUE_PURGE_OK, purgeOkPayload);
            }
        } catch (Exception e) {
            logger.error("Failed to purge queue: {}", queueName, e);
        }
    }

    private void handleQueueDelete(ByteBuf payload) {
        payload.skipBytes(2); // reserved
        String queueName = AmqpCodec.decodeShortString(payload);
        // In AMQP 0-9-1, if-unused, if-empty, and nowait are bit flags in a single byte
        byte flags = payload.readByte();
        boolean ifUnused = (flags & 0x01) != 0;
        boolean ifEmpty = (flags & 0x02) != 0;
        boolean noWait = (flags & 0x04) != 0;

        logger.debug("Queue Delete: name={}, ifUnused={}, ifEmpty={}", queueName, ifUnused, ifEmpty);

        try {
            com.amqp.security.User user = connection.getUser();
            if (user == null) {
                user = broker.getAuthenticationManager().getUser("guest");
            }

            int deletedCount = broker.deleteQueue(connection.getVirtualHost(), user, queueName, ifUnused, ifEmpty);

            if (!noWait) {
                ByteBuf deleteOkPayload = Unpooled.buffer();
                deleteOkPayload.writeInt(deletedCount);
                connection.sendMethod(channelNumber, AmqpConstants.CLASS_QUEUE,
                                     AmqpConstants.METHOD_QUEUE_DELETE_OK, deleteOkPayload);
            }
        } catch (Exception e) {
            logger.error("Failed to delete queue: {}", queueName, e);
            // Send Delete-Ok with 0 count on error
            if (!AmqpCodec.decodeBoolean(payload)) {
                ByteBuf deleteOkPayload = Unpooled.buffer();
                deleteOkPayload.writeInt(0);
                connection.sendMethod(channelNumber, AmqpConstants.CLASS_QUEUE,
                                     AmqpConstants.METHOD_QUEUE_DELETE_OK, deleteOkPayload);
            }
        }
    }
    
    private void handleBasicPublish(ByteBuf payload) {
        payload.skipBytes(2); // reserved
        String exchangeName = AmqpCodec.decodeShortString(payload);
        String routingKey = AmqpCodec.decodeShortString(payload);
        // mandatory and immediate are packed as bit flags in a single byte
        byte flags = payload.readByte();
        boolean mandatory = (flags & 0x01) != 0;
        boolean immediate = (flags & 0x02) != 0;

        logger.debug("Basic Publish: exchange={}, routingKey={}", exchangeName, routingKey);

        // Clear any existing publish context and start new one - synchronized to prevent race conditions
        synchronized (publishLock) {
            if (currentPublishContext != null) {
                currentPublishContext.releaseBuffer();
            }
            currentPublishContext = new PublishContext(exchangeName, routingKey, mandatory, immediate);
        }
    }
    
    private void handleBasicConsume(ByteBuf payload) {
        // Parse Basic.Consume parameters according to AMQP 0-9-1 spec
        payload.skipBytes(2); // reserved
        String queueName = AmqpCodec.decodeShortString(payload);
        String consumerTag = AmqpCodec.decodeShortString(payload);

        // Parse flags
        byte flags = payload.readByte();
        boolean noLocal = (flags & 0x01) != 0;
        boolean noAck = (flags & 0x02) != 0;
        boolean exclusive = (flags & 0x04) != 0;
        boolean noWait = (flags & 0x08) != 0;

        // Parse arguments (optional table)
        java.util.Map<String, Object> arguments = new java.util.HashMap<>();
        if (payload.isReadable()) {
            try {
                // Skip arguments parsing for now (would need table decoder)
                // arguments = AmqpCodec.decodeTable(payload);
            } catch (Exception e) {
                logger.warn("Failed to parse consumer arguments", e);
            }
        }

        // Generate consumer tag if not provided
        if (consumerTag == null || consumerTag.isEmpty()) {
            consumerTag = "ctag-" + System.currentTimeMillis() + "-" + channelNumber;
        }

        logger.debug("Basic Consume: queue={}, consumerTag={}, noAck={}, exclusive={}",
                    queueName, consumerTag, noAck, exclusive);

        try {
            // Register consumer with broker
            broker.addConsumer(connection.getVirtualHost(), queueName, consumerTag,
                             channelNumber, noAck, exclusive, arguments, connection);

            // Add to channel's consumer tracking
            addConsumer(consumerTag, queueName);

            // Send ConsumeOk response
            ByteBuf consumeOkPayload = Unpooled.buffer();
            AmqpCodec.encodeShortString(consumeOkPayload, consumerTag);
            connection.sendMethod(channelNumber, (short) 60, (short) 21, consumeOkPayload);

            logger.info("Consumer registered successfully: {} for queue: {}", consumerTag, queueName);
        } catch (Exception e) {
            logger.error("Failed to register consumer for queue: {}", queueName, e);
            // TODO: Send error frame to client
        }
    }

    private void handleBasicGet(ByteBuf payload) {
        // Parse Basic.Get parameters according to AMQP 0-9-1 spec
        payload.skipBytes(2); // reserved
        String queueName = AmqpCodec.decodeShortString(payload);
        boolean noAck = AmqpCodec.decodeBoolean(payload);

        logger.debug("Basic Get: queue={}, noAck={}", queueName, noAck);

        try {
            // Get user from connection
            com.amqp.security.User user = connection.getUser();
            if (user == null) {
                user = broker.getAuthenticationManager().getUser("guest");
            }

            // Try to consume a message from the queue
            com.amqp.model.Message message = broker.consumeMessage(
                connection.getVirtualHost(),
                user,
                queueName
            );

            if (message != null) {
                // Message found - send Basic.Get-Ok + content
                long deliveryTag = deliveryTagGenerator.incrementAndGet();

                // Track unacknowledged message if noAck=false
                if (!noAck) {
                    UnackedMessage unacked = new UnackedMessage(
                        message, queueName, connection.getVirtualHost(), false
                    );
                    unackedMessages.put(deliveryTag, unacked);
                }

                // Send Basic.Get-Ok (method 71)
                ByteBuf getOkPayload = Unpooled.buffer();
                getOkPayload.writeLong(deliveryTag); // delivery-tag
                AmqpCodec.encodeBoolean(getOkPayload, false); // redelivered
                AmqpCodec.encodeShortString(getOkPayload, ""); // exchange (not stored)
                AmqpCodec.encodeShortString(getOkPayload, message.getRoutingKey() != null ? message.getRoutingKey() : "");
                getOkPayload.writeInt(0); // message-count (approximate, we don't track)

                connection.sendMethod(channelNumber, (short) 60, (short) 71, getOkPayload);

                // Send Content Header
                long bodySize = message.getBody() != null ? message.getBody().length : 0;
                ByteBuf headerPayload = Unpooled.buffer();

                // Property flags (simplified encoding)
                short propertyFlags = 0;
                Short deliveryMode = message.getDeliveryMode();
                Short priority = message.getPriority();
                Long timestamp = message.getTimestamp();

                if (message.getContentType() != null) propertyFlags |= (1 << 15);
                if (message.getContentEncoding() != null) propertyFlags |= (1 << 14);
                if (deliveryMode != null) propertyFlags |= (1 << 12);
                if (priority != null) propertyFlags |= (1 << 11);
                if (message.getCorrelationId() != null) propertyFlags |= (1 << 10);
                if (message.getReplyTo() != null) propertyFlags |= (1 << 9);
                if (message.getExpiration() != null) propertyFlags |= (1 << 8);
                if (message.getMessageId() != null) propertyFlags |= (1 << 7);
                if (timestamp != null) propertyFlags |= (1 << 6);
                if (message.getType() != null) propertyFlags |= (1 << 5);
                if (message.getUserId() != null) propertyFlags |= (1 << 4);
                if (message.getAppId() != null) propertyFlags |= (1 << 3);

                headerPayload.writeShort(propertyFlags);

                // Encode properties
                if (message.getContentType() != null) {
                    AmqpCodec.encodeShortString(headerPayload, message.getContentType());
                }
                if (message.getContentEncoding() != null) {
                    AmqpCodec.encodeShortString(headerPayload, message.getContentEncoding());
                }
                if (deliveryMode != null) {
                    headerPayload.writeByte(deliveryMode);
                }
                if (priority != null) {
                    headerPayload.writeByte(priority);
                }
                if (message.getCorrelationId() != null) {
                    AmqpCodec.encodeShortString(headerPayload, message.getCorrelationId());
                }
                if (message.getReplyTo() != null) {
                    AmqpCodec.encodeShortString(headerPayload, message.getReplyTo());
                }
                if (message.getExpiration() != null) {
                    AmqpCodec.encodeShortString(headerPayload, message.getExpiration());
                }
                if (message.getMessageId() != null) {
                    AmqpCodec.encodeShortString(headerPayload, message.getMessageId());
                }
                if (timestamp != null) {
                    headerPayload.writeLong(timestamp);
                }
                if (message.getType() != null) {
                    AmqpCodec.encodeShortString(headerPayload, message.getType());
                }
                if (message.getUserId() != null) {
                    AmqpCodec.encodeShortString(headerPayload, message.getUserId());
                }
                if (message.getAppId() != null) {
                    AmqpCodec.encodeShortString(headerPayload, message.getAppId());
                }

                connection.sendContentHeader(channelNumber, (short) 60, bodySize, headerPayload);

                // Send Content Body
                if (message.getBody() != null && message.getBody().length > 0) {
                    ByteBuf bodyBuf = Unpooled.wrappedBuffer(message.getBody());
                    connection.sendContentBody(channelNumber, bodyBuf);
                }

                logger.debug("Basic.Get: Delivered message from queue: {}", queueName);
            } else {
                // No message available - send Basic.Get-Empty (method 72)
                ByteBuf getEmptyPayload = Unpooled.buffer();
                AmqpCodec.encodeShortString(getEmptyPayload, ""); // cluster-id (deprecated, always empty)

                connection.sendMethod(channelNumber, (short) 60, (short) 72, getEmptyPayload);

                logger.debug("Basic.Get: Queue empty: {}", queueName);
            }
        } catch (Exception e) {
            logger.error("Failed to handle Basic.Get for queue: {}", queueName, e);
            // Send Get-Empty on error
            ByteBuf getEmptyPayload = Unpooled.buffer();
            AmqpCodec.encodeShortString(getEmptyPayload, "");
            connection.sendMethod(channelNumber, (short) 60, (short) 72, getEmptyPayload);
        }
    }

    private void handleBasicAck(ByteBuf payload) {
        long deliveryTag = payload.readLong();
        boolean multiple = AmqpCodec.decodeBoolean(payload);

        logger.debug("Basic Ack: deliveryTag={}, multiple={}", deliveryTag, multiple);

        // Buffer if in transaction, otherwise execute immediately
        synchronized (transactionLock) {
            if (inTransaction) {
                transactionBuffer.add(new AckAction(deliveryTag, multiple));
                logger.debug("Buffered ack in transaction: deliveryTag={}, multiple={}", deliveryTag, multiple);
                return;
            }
        }
        acknowledgeMessage(deliveryTag, multiple);
    }

    /**
     * Acknowledge message(s). If multiple is true, acknowledges all messages up to and including deliveryTag.
     */
    private void acknowledgeMessage(long deliveryTag, boolean multiple) {
        if (multiple) {
            // Acknowledge all messages up to and including deliveryTag
            Map<Long, UnackedMessage> toRemove = new HashMap<>();
            for (Map.Entry<Long, UnackedMessage> entry : unackedMessages.entrySet()) {
                if (entry.getKey() <= deliveryTag) {
                    toRemove.put(entry.getKey(), entry.getValue());
                }
            }

            for (Map.Entry<Long, UnackedMessage> entry : toRemove.entrySet()) {
                unackedMessages.remove(entry.getKey());
                acknowledgeMessage(entry.getKey(), entry.getValue());
            }

            logger.debug("Acknowledged {} messages up to deliveryTag={}", toRemove.size(), deliveryTag);
        } else {
            // Acknowledge single message
            UnackedMessage unacked = unackedMessages.remove(deliveryTag);
            if (unacked != null) {
                acknowledgeMessage(deliveryTag, unacked);
            } else {
                logger.warn("Attempted to ack unknown deliveryTag: {}", deliveryTag);
            }
        }
    }

    private void acknowledgeMessage(long deliveryTag, UnackedMessage unacked) {
        // Delete message from persistence if it was durable
        broker.acknowledgeMessage(unacked.vhost, unacked.queueName, unacked.message);
        logger.debug("Acknowledged message: deliveryTag={}, queue={}", deliveryTag, unacked.queueName);
    }

    private void handleBasicReject(ByteBuf payload) {
        long deliveryTag = payload.readLong();
        boolean requeue = AmqpCodec.decodeBoolean(payload);

        logger.debug("Basic Reject: deliveryTag={}, requeue={}", deliveryTag, requeue);

        // Buffer if in transaction, otherwise execute immediately
        synchronized (transactionLock) {
            if (inTransaction) {
                transactionBuffer.add(new RejectAction(deliveryTag, requeue));
                logger.debug("Buffered reject in transaction: deliveryTag={}, requeue={}", deliveryTag, requeue);
                return;
            }
        }
        rejectMessage(deliveryTag, requeue);
    }

    private void rejectMessage(long deliveryTag, boolean requeue) {
        UnackedMessage unacked = unackedMessages.remove(deliveryTag);
        if (unacked != null) {
            if (requeue) {
                // Put message back in queue
                broker.requeueMessage(unacked.vhost, unacked.queueName, unacked.message);
                logger.debug("Requeued rejected message: deliveryTag={}, queue={}",
                           deliveryTag, unacked.queueName);
            } else {
                // Discard message - acknowledge it to remove from persistence
                broker.acknowledgeMessage(unacked.vhost, unacked.queueName, unacked.message);
                logger.debug("Discarded rejected message: deliveryTag={}, queue={}",
                           deliveryTag, unacked.queueName);
            }
        } else {
            logger.warn("Attempted to reject unknown deliveryTag: {}", deliveryTag);
        }
    }

    private void handleBasicNack(ByteBuf payload) {
        long deliveryTag = payload.readLong();
        byte flags = payload.readByte();
        boolean multiple = (flags & 0x01) != 0;
        boolean requeue = (flags & 0x02) != 0;

        logger.debug("Basic Nack: deliveryTag={}, multiple={}, requeue={}", deliveryTag, multiple, requeue);

        if (multiple) {
            // Nack all messages up to and including deliveryTag
            Map<Long, UnackedMessage> toReject = new HashMap<>();
            for (Map.Entry<Long, UnackedMessage> entry : unackedMessages.entrySet()) {
                if (entry.getKey() <= deliveryTag) {
                    toReject.put(entry.getKey(), entry.getValue());
                }
            }

            for (Map.Entry<Long, UnackedMessage> entry : toReject.entrySet()) {
                unackedMessages.remove(entry.getKey());
                UnackedMessage unacked = entry.getValue();

                if (requeue) {
                    broker.requeueMessage(unacked.vhost, unacked.queueName, unacked.message);
                } else {
                    broker.acknowledgeMessage(unacked.vhost, unacked.queueName, unacked.message);
                }
            }

            logger.debug("Nacked {} messages up to deliveryTag={}, requeue={}",
                       toReject.size(), deliveryTag, requeue);
        } else {
            // Nack single message
            UnackedMessage unacked = unackedMessages.remove(deliveryTag);
            if (unacked != null) {
                if (requeue) {
                    broker.requeueMessage(unacked.vhost, unacked.queueName, unacked.message);
                    logger.debug("Requeued nacked message: deliveryTag={}", deliveryTag);
                } else {
                    broker.acknowledgeMessage(unacked.vhost, unacked.queueName, unacked.message);
                    logger.debug("Discarded nacked message: deliveryTag={}", deliveryTag);
                }
            } else {
                logger.warn("Attempted to nack unknown deliveryTag: {}", deliveryTag);
            }
        }
    }
    
    private void handleHeaderFrame(AmqpFrame frame) {
        synchronized (publishLock) {
            if (currentPublishContext == null) {
                logger.warn("Received header frame without publish context on channel: {}", channelNumber);
                return;
            }

            ByteBuf payload = frame.getPayload();
            short classId = payload.readShort();
            payload.readShort(); // weight (unused)
            long bodySize = payload.readLong();

            currentPublishContext.expectedBodySize = bodySize;

            // Parse content properties
            short propertyFlags = payload.readShort();
            Message message = currentPublishContext.message;

        // Decode properties based on flags (bit 15 is highest)
        if ((propertyFlags & (1 << 15)) != 0) {
            message.setContentType(AmqpCodec.decodeShortString(payload));
        }
        if ((propertyFlags & (1 << 14)) != 0) {
            message.setContentEncoding(AmqpCodec.decodeShortString(payload));
        }
        if ((propertyFlags & (1 << 13)) != 0) {
            // headers table - skip for now
            payload.skipBytes(4); // table size
        }
        if ((propertyFlags & (1 << 12)) != 0) {
            message.setDeliveryMode(payload.readByte());
        }
        if ((propertyFlags & (1 << 11)) != 0) {
            message.setPriority(payload.readByte());
        }
        if ((propertyFlags & (1 << 10)) != 0) {
            message.setCorrelationId(AmqpCodec.decodeShortString(payload));
        }
        if ((propertyFlags & (1 << 9)) != 0) {
            message.setReplyTo(AmqpCodec.decodeShortString(payload));
        }
        if ((propertyFlags & (1 << 8)) != 0) {
            message.setExpiration(AmqpCodec.decodeShortString(payload));
        }
        if ((propertyFlags & (1 << 7)) != 0) {
            message.setMessageId(AmqpCodec.decodeShortString(payload));
        }
        if ((propertyFlags & (1 << 6)) != 0) {
            message.setTimestamp(payload.readLong());
        }
        if ((propertyFlags & (1 << 5)) != 0) {
            message.setType(AmqpCodec.decodeShortString(payload));
        }
        if ((propertyFlags & (1 << 4)) != 0) {
            message.setUserId(AmqpCodec.decodeShortString(payload));
        }
        if ((propertyFlags & (1 << 3)) != 0) {
            message.setAppId(AmqpCodec.decodeShortString(payload));
        }

            // Initialize body buffer if needed
            if (bodySize > 0) {
                currentPublishContext.bodyBuffer = Unpooled.buffer((int) bodySize);
            } else {
                // Zero-body message - publish immediately
                logger.debug("Received zero-body message, publishing immediately");
                completePublishLocked();
                return;
            }

            logger.debug("Received content header: bodySize={}, contentType={}",
                        bodySize, message.getContentType());
        } // end synchronized
    }

    private void handleBodyFrame(AmqpFrame frame) {
        synchronized (publishLock) {
            if (currentPublishContext == null) {
                logger.warn("Received body frame without publish context on channel: {}", channelNumber);
                return;
            }

            ByteBuf payload = frame.getPayload();

            // Append body data
            if (currentPublishContext.bodyBuffer != null) {
                currentPublishContext.bodyBuffer.writeBytes(payload);
            }

            // Check if we've received the complete message
            long receivedSize = currentPublishContext.bodyBuffer != null ?
                               currentPublishContext.bodyBuffer.readableBytes() : 0;
            long expectedSize = currentPublishContext.expectedBodySize;

            logger.debug("Received content body: size={}, total={}/{}",
                        payload.readableBytes(), receivedSize, expectedSize);

            if (receivedSize >= expectedSize) {
                // Message complete - publish it
                completePublishLocked();
            }
        }
    }

    // Must be called while holding publishLock
    private void completePublishLocked() {
        PublishContext ctx = currentPublishContext;
        if (ctx == null) {
            return;
        }

        try {
            // Copy body buffer to byte array
            if (ctx.bodyBuffer != null && ctx.bodyBuffer.readableBytes() > 0) {
                byte[] body = new byte[ctx.bodyBuffer.readableBytes()];
                ctx.bodyBuffer.readBytes(body);
                ctx.message.setBody(body);
            } else {
                ctx.message.setBody(new byte[0]);
            }

            // Get user from connection
            com.amqp.security.User user = connection.getUser();
            if (user == null) {
                user = broker.getAuthenticationManager().getUser("guest");
            }

            // Publish the message - buffer if in transaction, otherwise execute immediately
            boolean buffered = false;
            synchronized (transactionLock) {
                if (inTransaction) {
                    transactionBuffer.add(new PublishAction(
                        connection.getVirtualHost(),
                        user,
                        ctx.exchangeName,
                        ctx.routingKey,
                        ctx.message
                    ));
                    buffered = true;
                }
            }

            if (buffered) {
                logger.debug("Buffered publish in transaction: exchange={}, routingKey={}",
                           ctx.exchangeName, ctx.routingKey);
            } else {
                broker.publishMessage(
                    connection.getVirtualHost(),
                    user,
                    ctx.exchangeName,
                    ctx.routingKey,
                    ctx.message
                );
                logger.info("Published message: exchange={}, routingKey={}, size={}",
                           ctx.exchangeName, ctx.routingKey, ctx.message.getBody().length);
            }
        } catch (Exception e) {
            logger.error("Failed to publish message", e);
        } finally {
            // Clean up
            ctx.releaseBuffer();
            currentPublishContext = null;
        }
    }
    
    private void handleHeartbeat(AmqpFrame frame) {
        logger.trace("Received heartbeat on channel: {}", channelNumber);
        connection.recordHeartbeatReceived();
    }
    
    public void close() {
        // Synchronize on connection to prevent race with message delivery
        synchronized (connection) {
            open.set(false);

            // Cancel all consumers on this channel FIRST
            // This prevents deliveries to a closed channel
            broker.cancelConsumersForChannel(connection, channelNumber);
            consumers.clear();

            // Requeue all unacknowledged messages
            if (!unackedMessages.isEmpty()) {
                logger.info("Requeuing {} unacknowledged messages on channel close", unackedMessages.size());
                for (Map.Entry<Long, UnackedMessage> entry : unackedMessages.entrySet()) {
                    UnackedMessage unacked = entry.getValue();
                    broker.requeueMessage(unacked.vhost, unacked.queueName, unacked.message);
                }
                unackedMessages.clear();
            }

            // Clean up any pending publish context - synchronized to prevent race with publish
            synchronized (publishLock) {
                if (currentPublishContext != null) {
                    currentPublishContext.releaseBuffer();
                    currentPublishContext = null;
                }
            }

            // Clean up any pending transaction
            synchronized (transactionLock) {
                transactionBuffer.clear();
                inTransaction = false;
            }

            logger.debug("Channel {} closed", channelNumber);
        }
    }
    
    public boolean isOpen() {
        return open.get();
    }
    public short getChannelNumber() {
        return channelNumber;
    }
    
    public void addConsumer(String consumerTag, String queueName) {
        consumers.put(consumerTag, queueName);
        logger.debug("Added consumer {} for queue {}", consumerTag, queueName);
    }
    
    public void removeConsumer(String consumerTag) {
        consumers.remove(consumerTag);
        logger.debug("Removed consumer {}", consumerTag);
    }
    
    public boolean hasConsumer(String consumerTag) {
        return consumers.containsKey(consumerTag);
    }

    /**
     * Track a message delivery for acknowledgement.
     * Called by MessageDeliveryService when delivering messages to consumers.
     */
    public long trackDelivery(Message message, String queueName, String vhost, boolean redelivered) {
        long deliveryTag = deliveryTagGenerator.incrementAndGet();
        UnackedMessage unacked = new UnackedMessage(message, queueName, vhost, redelivered);
        unackedMessages.put(deliveryTag, unacked);
        logger.debug("Tracking delivery: deliveryTag={}, queue={}, messageId={}",
                   deliveryTag, queueName, message.getId());
        return deliveryTag;
    }

    /**
     * Get count of unacknowledged messages on this channel.
     */
    public int getUnackedMessageCount() {
        return unackedMessages.size();
    }
    
    public void setQoS(int prefetchCount) {
        this.prefetchCount = prefetchCount;
        logger.debug("Set prefetch count to {}", prefetchCount);
    }
    
    public int getPrefetchCount() {
        return prefetchCount;
    }
    
    public void txSelect() {
        synchronized (transactionLock) {
            inTransaction = true;
            transactionBuffer.clear(); // Start with clean buffer
        }
        logger.debug("Transaction started on channel {}", channelNumber);
    }

    public void txCommit() {
        List<TransactionAction> actionsToExecute;
        synchronized (transactionLock) {
            if (!inTransaction) {
                throw new IllegalStateException("No transaction in progress");
            }

            // Copy actions and clear state while holding lock
            actionsToExecute = new ArrayList<>(transactionBuffer);
            transactionBuffer.clear();
            inTransaction = false;
        }

        // Execute actions outside of lock to avoid deadlocks
        int actionCount = actionsToExecute.size();
        for (TransactionAction action : actionsToExecute) {
            try {
                action.execute();
            } catch (Exception e) {
                logger.error("Error executing transaction action during commit", e);
                // In AMQP, if any action fails during commit, the entire transaction should be rolled back
                // For now, we log and continue, but a production implementation should handle this better
            }
        }

        logger.info("Transaction committed on channel {}: {} actions executed", channelNumber, actionCount);
    }

    public void txRollback() {
        int actionCount;
        synchronized (transactionLock) {
            if (!inTransaction) {
                throw new IllegalStateException("No transaction in progress");
            }

            // Discard all buffered actions
            actionCount = transactionBuffer.size();
            transactionBuffer.clear();
            inTransaction = false;
        }
        logger.info("Transaction rolled back on channel {}: {} actions discarded", channelNumber, actionCount);
    }
}