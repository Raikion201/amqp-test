package com.amqp.protocol.v10.server;

import com.amqp.protocol.v10.frame.Amqp10FrameDecoder;
import com.amqp.protocol.v10.frame.Amqp10FrameEncoder;
import com.amqp.protocol.v10.frame.Amqp10ProtocolDecoder;
import com.amqp.security.sasl.PlainMechanism;
import com.amqp.security.sasl.AnonymousMechanism;
import com.amqp.security.sasl.SaslNegotiator;
import com.amqp.security.sasl.amqp10.Sasl10Handler;
import com.amqp.server.AmqpBroker;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * AMQP 1.0 Server.
 *
 * Netty-based server that handles AMQP 1.0 protocol connections.
 * Supports SASL authentication with PLAIN and ANONYMOUS mechanisms.
 */
public class Amqp10Server {

    private static final Logger log = LoggerFactory.getLogger(Amqp10Server.class);

    public static final int DEFAULT_PORT = 5671;
    public static final int DEFAULT_TLS_PORT = 5674;

    private final AmqpBroker broker;
    private final int port;
    private final SslContext sslContext;
    private final String containerId;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    // Server configuration
    private int maxFrameSize = 16384;
    private int idleTimeout = 60000;
    private boolean requireSasl = true;

    public Amqp10Server(AmqpBroker broker, int port) {
        this(broker, port, null);
    }

    public Amqp10Server(AmqpBroker broker, int port, SslContext sslContext) {
        this.broker = broker;
        this.port = port;
        this.sslContext = sslContext;
        this.containerId = "amqp-server-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Creates a SASL negotiator with mechanisms that use the broker's authentication.
     */
    private SaslNegotiator createSaslNegotiator() {
        SaslNegotiator negotiator = new SaslNegotiator();

        // Add PLAIN mechanism using broker's authentication
        negotiator.addMechanism(new PlainMechanism((username, password) -> {
            return broker.authenticate(username, password);
        }));

        // Add ANONYMOUS mechanism if guest user is enabled
        if (broker.isGuestUserEnabled()) {
            negotiator.addMechanism(new AnonymousMechanism());
        }

        return negotiator;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();

                            // TLS if configured
                            if (sslContext != null) {
                                pipeline.addLast("ssl", sslContext.newHandler(ch.alloc()));
                            }

                            // Idle state handler for heartbeats
                            if (idleTimeout > 0) {
                                pipeline.addLast("idle", new IdleStateHandler(
                                        idleTimeout * 2, idleTimeout, 0, TimeUnit.MILLISECONDS));
                            }

                            // Protocol header decoder (handles AMQP/SASL header negotiation)
                            pipeline.addLast("protocol", new Amqp10ProtocolDecoder(requireSasl));

                            // Frame codec
                            pipeline.addLast("frameDecoder", new Amqp10FrameDecoder(maxFrameSize));
                            pipeline.addLast("frameEncoder", new Amqp10FrameEncoder());

                            // SASL handler (before connection handler)
                            if (requireSasl) {
                                SaslNegotiator negotiator = createSaslNegotiator();
                                pipeline.addLast("sasl", new Sasl10Handler(negotiator, requireSasl));
                            }

                            // Connection handler
                            pipeline.addLast("handler", new Amqp10ConnectionHandler(
                                    broker, containerId, maxFrameSize, requireSasl));
                        }
                    });

            ChannelFuture future = bootstrap.bind(port).sync();
            serverChannel = future.channel();

            log.info("AMQP 1.0 server started on port {} (TLS: {}, SASL: {})",
                    port, sslContext != null, requireSasl);

        } catch (Exception e) {
            log.error("Failed to start AMQP 1.0 server", e);
            shutdown();
            throw e;
        }
    }

    public void shutdown() {
        log.info("Shutting down AMQP 1.0 server on port {}", port);

        if (serverChannel != null) {
            serverChannel.close();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    public void awaitShutdown() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.closeFuture().sync();
        }
    }

    // Configuration
    public int getPort() {
        return port;
    }

    public String getContainerId() {
        return containerId;
    }

    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public boolean isRequireSasl() {
        return requireSasl;
    }

    public void setRequireSasl(boolean requireSasl) {
        this.requireSasl = requireSasl;
    }

    public boolean isRunning() {
        return serverChannel != null && serverChannel.isActive();
    }
}
