package com.amqp.handler;

import com.amqp.amqp.AmqpFrame;
import com.amqp.amqp.AmqpCodec;
import com.amqp.amqp.AmqpConstants;
import com.amqp.server.AmqpBroker;
import com.amqp.connection.AmqpConnection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class AmqpConnectionHandler extends SimpleChannelInboundHandler<AmqpFrame> {
    private static final Logger logger = LoggerFactory.getLogger(AmqpConnectionHandler.class);

    private static final int DEFAULT_MAX_CONNECTIONS = 1000;

    private final AmqpBroker broker;
    private final ConcurrentMap<SocketAddress, AmqpConnection> connections = new ConcurrentHashMap<>();
    private final int maxConnections;
    private AmqpConnection connection;

    public AmqpConnectionHandler(AmqpBroker broker) {
        this(broker, DEFAULT_MAX_CONNECTIONS);
    }

    public AmqpConnectionHandler(AmqpBroker broker, int maxConnections) {
        this.broker = broker;
        this.maxConnections = maxConnections;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("New connection from {}", ctx.channel().remoteAddress());

        // Enforce connection limit
        if (connections.size() >= maxConnections) {
            logger.warn("Connection limit reached ({}/{}). Rejecting connection from {}",
                       connections.size(), maxConnections, ctx.channel().remoteAddress());
            sendConnectionCloseAndDisconnect(ctx, AmqpConstants.REPLY_CONNECTION_FORCED,
                                           "CONNECTION_FORCED - Connection limit exceeded");
            return;
        }

        connection = new AmqpConnection(ctx.channel(), broker);
        connections.put(ctx.channel().remoteAddress(), connection);

        sendConnectionStart(ctx);
    }

    private void sendConnectionCloseAndDisconnect(ChannelHandlerContext ctx, int replyCode, String replyText) {
        try {
            ByteBuf payload = Unpooled.buffer();
            payload.writeShort(replyCode); // reply-code
            AmqpCodec.encodeShortString(payload, replyText); // reply-text
            payload.writeShort(0); // class-id
            payload.writeShort(0); // method-id

            AmqpFrame closeFrame = new AmqpFrame(AmqpFrame.FrameType.METHOD.getValue(), (short) 0, payload);
            ctx.writeAndFlush(closeFrame);
        } catch (Exception e) {
            logger.error("Error sending connection close", e);
        } finally {
            // Close the connection
            ctx.close();
        }
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Connection closed: {}", ctx.channel().remoteAddress());
        connections.remove(ctx.channel().remoteAddress());
        if (connection != null) {
            connection.close();
        }
    }
    
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, AmqpFrame frame) throws Exception {
        if (connection != null) {
            connection.handleFrame(frame);
        }
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Exception in connection handler", cause);
        ctx.close();
    }
    
    private void sendConnectionStart(ChannelHandlerContext ctx) {
        ByteBuf payload = Unpooled.buffer();
        
        payload.writeShort(10);
        payload.writeShort(10);
        
        payload.writeByte(0);
        payload.writeByte(9);
        
        payload.writeInt(0);
        
        AmqpCodec.encodeLongString(payload, "PLAIN");
        AmqpCodec.encodeLongString(payload, "en_US");
        
        AmqpFrame startFrame = new AmqpFrame(AmqpFrame.FrameType.METHOD.getValue(), (short) 0, payload);
        ctx.writeAndFlush(startFrame);
        
        logger.debug("Sent Connection.Start to {}", ctx.channel().remoteAddress());
    }
}