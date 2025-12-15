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

public class AmqpServer {
    private static final Logger logger = LoggerFactory.getLogger(AmqpServer.class);

    private final int port;
    private final AmqpBroker broker;
    private final boolean sslEnabled;
    private final SslContext sslContext;
    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public AmqpServer(int port, AmqpBroker broker) {
        this(port, broker, false, null, null);
    }

    public AmqpServer(int port, AmqpBroker broker, boolean sslEnabled,
                     String certPath, String keyPath) {
        this.port = port;
        this.broker = broker;
        this.sslEnabled = sslEnabled;

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
        broker.stop();
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        logger.info("AMQP Server stopped");
    }
}