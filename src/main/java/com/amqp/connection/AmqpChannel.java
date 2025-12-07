package com.amqp.connection;

import com.amqp.amqp.AmqpFrame;
import com.amqp.amqp.AmqpMethod;
import com.amqp.amqp.AmqpCodec;
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

public class AmqpChannel {
    private static final Logger logger = LoggerFactory.getLogger(AmqpChannel.class);
    
    private final short channelNumber;
    private final AmqpConnection connection;
    private final AmqpBroker broker;
    private final AtomicBoolean open;
    private final ConcurrentMap<String, String> consumers;
    private int prefetchCount = 0;
    private boolean inTransaction = false;
    
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
            case 10: // Connection
                handleConnectionMethod(methodId, payload);
                break;
            case 20: // Channel
                handleChannelMethod(methodId, payload);
                break;
            case 40: // Exchange
                handleExchangeMethod(methodId, payload);
                break;
            case 50: // Queue
                handleQueueMethod(methodId, payload);
                break;
            case 60: // Basic
                handleBasicMethod(methodId, payload);
                break;
            default:
                logger.warn("Unknown class ID: {}", classId);
        }
    }
    
    private void handleConnectionMethod(short methodId, ByteBuf payload) {
        switch (methodId) {
            case 11: // Start-Ok
                handleConnectionStartOk(payload);
                break;
            case 31: // Tune-Ok
                handleConnectionTuneOk(payload);
                break;
            case 40: // Open
                handleConnectionOpen(payload);
                break;
            case 50: // Close
                handleConnectionClose(payload);
                break;
        }
    }
    
    private void handleChannelMethod(short methodId, ByteBuf payload) {
        switch (methodId) {
            case 10: // Open
                handleChannelOpen(payload);
                break;
            case 40: // Close
                handleChannelClose(payload);
                break;
        }
    }
    
    private void handleExchangeMethod(short methodId, ByteBuf payload) {
        switch (methodId) {
            case 10: // Declare
                handleExchangeDeclare(payload);
                break;
            case 20: // Delete
                handleExchangeDelete(payload);
                break;
        }
    }
    
    private void handleQueueMethod(short methodId, ByteBuf payload) {
        switch (methodId) {
            case 10: // Declare
                handleQueueDeclare(payload);
                break;
            case 20: // Bind
                handleQueueBind(payload);
                break;
            case 40: // Delete
                handleQueueDelete(payload);
                break;
        }
    }
    
    private void handleBasicMethod(short methodId, ByteBuf payload) {
        switch (methodId) {
            case 40: // Publish
                handleBasicPublish(payload);
                break;
            case 20: // Consume
                handleBasicConsume(payload);
                break;
            case 80: // Ack
                handleBasicAck(payload);
                break;
        }
    }
    
    private void handleConnectionStartOk(ByteBuf payload) {
        payload.skipBytes(4);
        String mechanism = AmqpCodec.decodeLongString(payload);
        String response = AmqpCodec.decodeLongString(payload);
        String locale = AmqpCodec.decodeLongString(payload);
        
        logger.debug("Connection Start-Ok: mechanism={}, locale={}", mechanism, locale);
        
        ByteBuf tunePayload = Unpooled.buffer();
        tunePayload.writeShort(2047);
        tunePayload.writeInt(131072);
        tunePayload.writeShort(60);
        
        connection.sendMethod(channelNumber, (short) 10, (short) 30, tunePayload);
    }
    
    private void handleConnectionTuneOk(ByteBuf payload) {
        short channelMax = payload.readShort();
        int frameMax = payload.readInt();
        short heartbeat = payload.readShort();
        
        logger.debug("Connection Tune-Ok: channelMax={}, frameMax={}, heartbeat={}", 
                    channelMax, frameMax, heartbeat);
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
        payload.skipBytes(2);
        String exchangeName = AmqpCodec.decodeShortString(payload);
        String exchangeType = AmqpCodec.decodeShortString(payload);
        boolean passive = AmqpCodec.decodeBoolean(payload);
        boolean durable = AmqpCodec.decodeBoolean(payload);
        boolean autoDelete = AmqpCodec.decodeBoolean(payload);
        boolean internal = AmqpCodec.decodeBoolean(payload);
        boolean nowait = AmqpCodec.decodeBoolean(payload);
        
        logger.debug("Exchange Declare: name={}, type={}", exchangeName, exchangeType);
        
        try {
            Exchange.Type type = Exchange.Type.valueOf(exchangeType.toUpperCase());
            broker.declareExchange(exchangeName, type, durable, autoDelete, internal);
            
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
        payload.skipBytes(2);
        String queueName = AmqpCodec.decodeShortString(payload);
        boolean passive = AmqpCodec.decodeBoolean(payload);
        boolean durable = AmqpCodec.decodeBoolean(payload);
        boolean exclusive = AmqpCodec.decodeBoolean(payload);
        boolean autoDelete = AmqpCodec.decodeBoolean(payload);
        boolean nowait = AmqpCodec.decodeBoolean(payload);
        
        logger.debug("Queue Declare: name={}", queueName);
        
        try {
            Queue queue = broker.declareQueue(queueName, durable, exclusive, autoDelete);
            
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
            broker.bindQueue(queueName, exchangeName, routingKey);
            
            if (!nowait) {
                connection.sendMethod(channelNumber, (short) 50, (short) 21, null);
            }
        } catch (Exception e) {
            logger.error("Failed to bind queue: {}", queueName, e);
        }
    }
    
    private void handleQueueDelete(ByteBuf payload) {
        payload.skipBytes(2);
        String queueName = AmqpCodec.decodeShortString(payload);
        
        logger.debug("Queue Delete: name={}", queueName);
        
        ByteBuf deleteOkPayload = Unpooled.buffer();
        deleteOkPayload.writeInt(0);
        
        connection.sendMethod(channelNumber, (short) 50, (short) 41, deleteOkPayload);
    }
    
    private void handleBasicPublish(ByteBuf payload) {
        payload.skipBytes(2);
        String exchangeName = AmqpCodec.decodeShortString(payload);
        String routingKey = AmqpCodec.decodeShortString(payload);
        boolean mandatory = AmqpCodec.decodeBoolean(payload);
        boolean immediate = AmqpCodec.decodeBoolean(payload);
        
        logger.debug("Basic Publish: exchange={}, routingKey={}", exchangeName, routingKey);
    }
    
    private void handleBasicConsume(ByteBuf payload) {
        payload.skipBytes(2);
        String queueName = AmqpCodec.decodeShortString(payload);
        String consumerTag = AmqpCodec.decodeShortString(payload);
        
        logger.debug("Basic Consume: queue={}, consumerTag={}", queueName, consumerTag);
        
        ByteBuf consumeOkPayload = Unpooled.buffer();
        AmqpCodec.encodeShortString(consumeOkPayload, consumerTag);
        
        connection.sendMethod(channelNumber, (short) 60, (short) 21, consumeOkPayload);
    }
    
    private void handleBasicAck(ByteBuf payload) {
        long deliveryTag = payload.readLong();
        boolean multiple = AmqpCodec.decodeBoolean(payload);
        
        logger.debug("Basic Ack: deliveryTag={}, multiple={}", deliveryTag, multiple);
    }
    
    private void handleHeaderFrame(AmqpFrame frame) {
        logger.debug("Received content header on channel: {}", channelNumber);
    }
    
    private void handleBodyFrame(AmqpFrame frame) {
        logger.debug("Received content body on channel: {}", channelNumber);
    }
    
    private void handleHeartbeat(AmqpFrame frame) {
        logger.debug("Received heartbeat on channel: {}", channelNumber);
    }
    
    public void close() {
        open.set(false);
        logger.debug("Channel {} closed", channelNumber);
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
    
    public void acknowledgeMessage(long deliveryTag, boolean multiple) {
        logger.debug("Acknowledging message: deliveryTag={}, multiple={}", deliveryTag, multiple);
        // Implementation would track unacknowledged messages and remove them
    }
    
    public void rejectMessage(long deliveryTag, boolean requeue) {
        logger.debug("Rejecting message: deliveryTag={}, requeue={}", deliveryTag, requeue);
        // Implementation would handle message rejection
    }
    
    public void setQoS(int prefetchCount) {
        this.prefetchCount = prefetchCount;
        logger.debug("Set prefetch count to {}", prefetchCount);
    }
    
    public int getPrefetchCount() {
        return prefetchCount;
    }
    
    public void txSelect() {
        inTransaction = true;
        logger.debug("Transaction started on channel {}", channelNumber);
    }
    
    public void txCommit() {
        if (!inTransaction) {
            throw new IllegalStateException("No transaction in progress");
        }
        inTransaction = false;
        logger.debug("Transaction committed on channel {}", channelNumber);
    }
    
    public void txRollback() {
        if (!inTransaction) {
            throw new IllegalStateException("No transaction in progress");
        }
        inTransaction = false;
        logger.debug("Transaction rolled back on channel {}", channelNumber);
    }
}