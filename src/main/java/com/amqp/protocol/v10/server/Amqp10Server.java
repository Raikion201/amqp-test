package com.amqp.protocol.v10.server;

import com.amqp.protocol.v10.frame.Amqp10FrameDecoder;
import com.amqp.protocol.v10.frame.Amqp10FrameEncoder;
import com.amqp.protocol.v10.frame.Amqp10ProtocolDecoder;
import com.amqp.protocol.v10.security.Amqp10SecurityConfig;
import com.amqp.protocol.v10.security.ConnectionLimiter;
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
    private int maxFrameSize = 1024 * 1024; // 1MB to support large messages
    private int idleTimeout = 60000;
    private boolean requireSasl = true;

    // Security configuration
    private Amqp10SecurityConfig securityConfig;
    private ConnectionLimiter connectionLimiter;

    public Amqp10Server(AmqpBroker broker, int port) {
        this(broker, port, null);
    }

    public Amqp10Server(AmqpBroker broker, int port, SslContext sslContext) {
        this(broker, port, sslContext, Amqp10SecurityConfig.defaults());
    }

    public Amqp10Server(AmqpBroker broker, int port, SslContext sslContext,
                        Amqp10SecurityConfig securityConfig) {
        this.broker = broker;
        this.port = port;
        this.sslContext = sslContext;
        this.securityConfig = securityConfig;
        this.containerId = "amqp-server-" + UUID.randomUUID().toString().substring(0, 8);
        this.connectionLimiter = new ConnectionLimiter(securityConfig);
    }

    /**
     * Creates a SASL negotiator with mechanisms that use the broker's authentication.
     */
    private SaslNegotiator createSaslNegotiator() {
        SaslNegotiator negotiator = new SaslNegotiator();

        // Add PLAIN mechanism using broker's authentication
        // TLS requirement is enforced based on security config
        boolean requireTlsForPlain = securityConfig.isRequireTlsForPlainAuth();
        negotiator.addMechanism(new PlainMechanism((username, password) -> {
            return broker.authenticate(username, password);
        }, requireTlsForPlain));

        // Add ANONYMOUS mechanism if guest user is enabled and config allows it
        if (broker.isGuestUserEnabled() && securityConfig.isAllowAnonymous()) {
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

                            // Connection limiter (first in pipeline for early rejection)
                            pipeline.addLast("connectionLimiter", connectionLimiter);

                            // TLS if configured
                            if (sslContext != null) {
                                pipeline.addLast("ssl", sslContext.newHandler(ch.alloc()));
                            } else if (securityConfig.isRequireTlsForAllConnections()) {
                                // Reject non-TLS connections if TLS is required
                                log.warn("TLS required but not configured - rejecting connection from {}",
                                        ch.remoteAddress());
                                ch.close();
                                return;
                            }

                            // Idle state handler for heartbeats
                            // Use security config's validated timeout
                            long validatedTimeout = securityConfig.validateIdleTimeout(idleTimeout);
                            if (validatedTimeout > 0) {
                                pipeline.addLast("idle", new IdleStateHandler(
                                        validatedTimeout * 2, validatedTimeout, 0, TimeUnit.MILLISECONDS));
                            }

                            // Protocol header decoder (handles AMQP/SASL header negotiation)
                            pipeline.addLast("protocol", new Amqp10ProtocolDecoder(requireSasl));

                            // Frame codec - use max frame size from security config
                            int effectiveMaxFrameSize = (int) Math.min(maxFrameSize,
                                    securityConfig.getMaxFrameSize());
                            pipeline.addLast("frameDecoder", new Amqp10FrameDecoder(effectiveMaxFrameSize));
                            pipeline.addLast("frameEncoder", new Amqp10FrameEncoder());

                            // SASL handler (before connection handler)
                            // Always add SASL handler to support clients that want SASL,
                            // but mark as required only if configured
                            boolean saslRequired = requireSasl || securityConfig.isRequireAuthentication();
                            SaslNegotiator negotiator = createSaslNegotiator();
                            pipeline.addLast("sasl", new Sasl10Handler(negotiator, saslRequired));

                            // Connection handler with security config
                            pipeline.addLast("handler", new Amqp10ConnectionHandler(
                                    broker, containerId, effectiveMaxFrameSize, saslRequired));
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
        // When SASL is disabled, also update security config to be consistent
        if (!requireSasl) {
            securityConfig.setRequireAuthentication(false);
            securityConfig.setAllowAnonymous(true);
        }
    }

    public boolean isRunning() {
        return serverChannel != null && serverChannel.isActive();
    }

    public Amqp10SecurityConfig getSecurityConfig() {
        return securityConfig;
    }

    public void setSecurityConfig(Amqp10SecurityConfig securityConfig) {
        this.securityConfig = securityConfig;
        this.connectionLimiter = new ConnectionLimiter(securityConfig);
    }

    public ConnectionLimiter getConnectionLimiter() {
        return connectionLimiter;
    }

    /**
     * Get current connection statistics.
     */
    public ConnectionLimiter.Stats getConnectionStats() {
        return connectionLimiter.getStats();
    }
}
