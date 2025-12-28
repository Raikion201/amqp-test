package com.amqp.server;

import com.amqp.amqp.AmqpCodec;
import com.amqp.handler.AmqpConnectionHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AmqpServer {
    private static final Logger logger = LoggerFactory.getLogger(AmqpServer.class);

    // Connection limit constants
    private static final int DEFAULT_MAX_CONNECTIONS = 10000;
    private static final int GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = 15;

    private final int port;
    private final AmqpBroker broker;
    private final boolean sslEnabled;
    private final SslContext sslContext;
    private final int maxConnections;
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public AmqpServer(int port, AmqpBroker broker) {
        this(port, broker, false, null, null, DEFAULT_MAX_CONNECTIONS);
    }

    public AmqpServer(int port, AmqpBroker broker, boolean sslEnabled,
                     String certPath, String keyPath) {
        this(port, broker, sslEnabled, certPath, keyPath, DEFAULT_MAX_CONNECTIONS);
    }

    public AmqpServer(int port, AmqpBroker broker, boolean sslEnabled,
                     String certPath, String keyPath, int maxConnections) {
        this.port = port;
        this.broker = broker;
        this.sslEnabled = sslEnabled;
        this.maxConnections = maxConnections;

        // Initialize SSL if enabled
        if (sslEnabled && certPath != null && keyPath != null) {
            try {
                this.sslContext = SslContextBuilder.forServer(
                    new File(certPath), new File(keyPath)
                ).build();
                logger.info("SSL/TLS enabled for AMQP server");
            } catch (SSLException e) {
                throw new RuntimeException("Failed to initialize SSL context for AMQP server", e);
            }
        } else {
            this.sslContext = null;
        }
    }
    
    public void start() throws InterruptedException {
        // Start the broker's message delivery service
        broker.start();

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .option(ChannelOption.SO_BACKLOG, 100)
             .handler(new LoggingHandler(LogLevel.INFO))
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     // Check connection limit BEFORE adding handlers
                     int currentConnections = activeConnections.get();
                     if (currentConnections >= maxConnections) {
                         logger.warn("SECURITY: Connection rejected from {}: max connections ({}) exceeded",
                                 ch.remoteAddress(), maxConnections);
                         ch.close();
                         return;
                     }

                     // Track active connection
                     activeConnections.incrementAndGet();
                     logger.debug("New connection from {}, active connections: {}",
                             ch.remoteAddress(), activeConnections.get());

                     // Decrement counter when connection closes
                     ch.closeFuture().addListener(future -> {
                         int remaining = activeConnections.decrementAndGet();
                         logger.debug("Connection closed from {}, active connections: {}",
                                 ch.remoteAddress(), remaining);
                     });

                     ChannelPipeline pipeline = ch.pipeline();

                     // Add SSL handler if enabled
                     if (sslEnabled && sslContext != null) {
                         pipeline.addLast("ssl", sslContext.newHandler(ch.alloc()));
                         logger.debug("SSL handler added to pipeline for connection from {}", ch.remoteAddress());
                     }

                     pipeline.addLast("protocolHeaderDecoder", new AmqpCodec.ProtocolHeaderDecoder());
                     pipeline.addLast("frameEncoder", new AmqpCodec.AmqpFrameEncoder());
                     pipeline.addLast("connectionHandler", new AmqpConnectionHandler(broker));
                 }
             });

            ChannelFuture f = b.bind(port).sync();
            serverChannel = f.channel();

            logger.info("AMQP Server started on port {} (SSL: {})", port, sslEnabled);

            f.channel().closeFuture().sync();
        } finally {
            broker.stop();
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
    
    public void stop() {
        logger.info("Stopping AMQP Server gracefully...");

        // Stop accepting new connections first
        if (serverChannel != null && serverChannel.isOpen()) {
            try {
                serverChannel.close().sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while closing server channel");
            }
        }

        // Stop broker (allows in-flight messages to complete)
        broker.stop();

        // Shutdown event loops with proper timeout
        if (bossGroup != null && !bossGroup.isShutdown()) {
            try {
                bossGroup.shutdownGracefully(1, GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS).sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Boss group shutdown interrupted");
            }
        }

        if (workerGroup != null && !workerGroup.isShutdown()) {
            try {
                workerGroup.shutdownGracefully(1, GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS).sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Worker group shutdown interrupted");
            }
        }

        logger.info("AMQP Server stopped (final active connections: {})", activeConnections.get());
    }

    /**
     * Get the current number of active connections.
     */
    public int getActiveConnectionCount() {
        return activeConnections.get();
    }

    /**
     * Get the maximum allowed connections.
     */
    public int getMaxConnections() {
        return maxConnections;
    }
}