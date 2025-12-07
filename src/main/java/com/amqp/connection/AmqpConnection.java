package com.amqp.connection;

import com.amqp.amqp.AmqpFrame;
import com.amqp.amqp.AmqpMethod;
import com.amqp.server.AmqpBroker;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class AmqpConnection {
    private static final Logger logger = LoggerFactory.getLogger(AmqpConnection.class);
    
    private final Channel nettyChannel;
    private final AmqpBroker broker;
    private final ConcurrentMap<Short, AmqpChannel> channels;
    private final AtomicBoolean connected;
    private volatile String virtualHost = "/";
    private volatile String username;
    
    public AmqpConnection(Channel nettyChannel, AmqpBroker broker) {
        this.nettyChannel = nettyChannel;
        this.broker = broker;
        this.channels = new ConcurrentHashMap<>();
        this.connected = new AtomicBoolean(false);
        
        AmqpChannel mainChannel = new AmqpChannel((short) 0, this, broker);
        channels.put((short) 0, mainChannel);
    }
    
    public void handleFrame(AmqpFrame frame) {
        short channelNumber = frame.getChannel();
        AmqpChannel channel = channels.get(channelNumber);
        
        if (channel == null) {
            logger.warn("Received frame for unknown channel: {}", channelNumber);
            return;
        }
        
        try {
            channel.handleFrame(frame);
        } catch (Exception e) {
            logger.error("Error handling frame on channel {}", channelNumber, e);
            closeChannel(channelNumber);
        }
    }
    
    public AmqpChannel openChannel(short channelNumber) {
        if (channels.containsKey(channelNumber)) {
            throw new IllegalArgumentException("Channel already exists: " + channelNumber);
        }
        
        AmqpChannel channel = new AmqpChannel(channelNumber, this, broker);
        channels.put(channelNumber, channel);
        
        logger.debug("Opened channel: {}", channelNumber);
        return channel;
    }
    
    public void closeChannel(short channelNumber) {
        AmqpChannel channel = channels.remove(channelNumber);
        if (channel != null) {
            channel.close();
            logger.debug("Closed channel: {}", channelNumber);
        }
    }
    
    public void sendFrame(AmqpFrame frame) {
        if (nettyChannel.isActive()) {
            nettyChannel.writeAndFlush(frame);
        }
    }
    
    public void sendMethod(short channelNumber, short classId, short methodId, ByteBuf payload) {
        ByteBuf methodFrame = Unpooled.buffer();
        methodFrame.writeShort(classId);
        methodFrame.writeShort(methodId);
        if (payload != null) {
            methodFrame.writeBytes(payload);
        }
        
        AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.METHOD.getValue(), channelNumber, methodFrame);
        sendFrame(frame);
    }
    
    public void sendContentHeader(short channelNumber, short classId, long bodySize, ByteBuf properties) {
        ByteBuf headerFrame = Unpooled.buffer();
        headerFrame.writeShort(classId);
        headerFrame.writeShort(0);
        headerFrame.writeLong(bodySize);
        if (properties != null) {
            headerFrame.writeBytes(properties);
        }
        
        AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.HEADER.getValue(), channelNumber, headerFrame);
        sendFrame(frame);
    }
    
    public void sendContentBody(short channelNumber, ByteBuf body) {
        AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.BODY.getValue(), channelNumber, body);
        sendFrame(frame);
    }
    
    public void close() {
        connected.set(false);
        
        for (AmqpChannel channel : channels.values()) {
            channel.close();
        }
        channels.clear();
        
        if (nettyChannel.isActive()) {
            nettyChannel.close();
        }
        
        logger.info("Connection closed");
    }
    
    public boolean isConnected() {
        return connected.get() && nettyChannel.isActive();
    }
    
    public void setConnected(boolean connected) {
        this.connected.set(connected);
    }
    
    public String getVirtualHost() {
        return virtualHost;
    }
    
    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }
    
    public String getUsername() {
        return username;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public AmqpBroker getBroker() {
        return broker;
    }
}