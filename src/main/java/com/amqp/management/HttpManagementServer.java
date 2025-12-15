package com.amqp.management;

import com.amqp.server.AmqpBroker;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * HTTP Management REST API Server for RabbitMQ-compatible management interface.
 * Provides REST endpoints for:
 * - Overview and cluster information
 * - Vhosts management
 * - Users and permissions
 * - Exchanges, queues, bindings
 * - Connections and channels
 * - Policies
 * - Health checks
 */
public class HttpManagementServer {
    private static final Logger logger = LoggerFactory.getLogger(HttpManagementServer.class);

    private final int port;
    private final ManagementApi managementApi;
    private final ObjectMapper objectMapper;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private Channel serverChannel;
    private final boolean sslEnabled;
    private final SslContext sslContext;

    public HttpManagementServer(int port, ManagementApi managementApi) {
        this(port, managementApi, false, null, null);
    }

    public HttpManagementServer(int port, ManagementApi managementApi, boolean sslEnabled,
                               String certPath, String keyPath) {
        this.port = port;
        this.managementApi = managementApi;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();
        this.sslEnabled = sslEnabled;

        // Initialize SSL if enabled
        if (sslEnabled && certPath != null && keyPath != null) {
            try {
                this.sslContext = SslContextBuilder.forServer(
                    new File(certPath), new File(keyPath)
                ).build();
            } catch (SSLException e) {
                throw new RuntimeException("Failed to initialize SSL context", e);
            }
        } else {
            this.sslContext = null;
        }
    }

    public void start() throws InterruptedException {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();

                    // Add SSL handler if enabled
                    if (sslEnabled && sslContext != null) {
                        pipeline.addLast(sslContext.newHandler(ch.alloc()));
                    }

                    pipeline.addLast(new HttpServerCodec());
                    pipeline.addLast(new HttpObjectAggregator(65536));
                    pipeline.addLast(new HttpManagementHandler());
                }
            })
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

        serverChannel = bootstrap.bind(port).sync().channel();
        logger.info("HTTP Management API started on port {} (SSL: {})", port, sslEnabled);
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        logger.info("HTTP Management API stopped");
    }

    /**
     * HTTP handler for management API requests.
     */
    private class HttpManagementHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            String uri = request.uri();
            HttpMethod method = request.method();

            logger.debug("Management API request: {} {}", method, uri);

            try {
                // Skip auth for health check and API root endpoints
                if (!uri.startsWith("/api/health") && !uri.equals("/api") && !uri.equals("/")) {
                    // Authenticate request using Basic Auth
                    com.amqp.security.User user = authenticateRequest(request);
                    if (user == null) {
                        sendAuthenticationRequired(ctx);
                        return;
                    }

                    // Check if user has administrator tag (required for management API)
                    if (!user.getTags().contains("administrator") && !user.getTags().contains("management")) {
                        sendJsonResponse(ctx, FORBIDDEN,
                            Map.of("error", "User must have 'administrator' or 'management' tag to access management API"));
                        return;
                    }
                }

                Object response = routeRequest(method, uri, request);
                sendJsonResponse(ctx, OK, response);
            } catch (IllegalArgumentException e) {
                sendJsonResponse(ctx, BAD_REQUEST, Map.of("error", e.getMessage()));
            } catch (SecurityException e) {
                sendJsonResponse(ctx, FORBIDDEN, Map.of("error", e.getMessage()));
            } catch (Exception e) {
                logger.error("Error handling management request", e);
                sendJsonResponse(ctx, INTERNAL_SERVER_ERROR,
                               Map.of("error", "Internal server error", "message", e.getMessage()));
            }
        }

        private com.amqp.security.User authenticateRequest(FullHttpRequest request) {
            String authHeader = request.headers().get(HttpHeaderNames.AUTHORIZATION);
            if (authHeader == null || !authHeader.startsWith("Basic ")) {
                return null;
            }

            try {
                // Decode Base64 Basic Auth credentials
                String base64Credentials = authHeader.substring("Basic ".length());
                String credentials = new String(Base64.getDecoder().decode(base64Credentials), StandardCharsets.UTF_8);
                String[] parts = credentials.split(":", 2);

                if (parts.length != 2) {
                    return null;
                }

                String username = parts[0];
                String password = parts[1];

                // Authenticate via management API's authentication manager
                return managementApi.getAuthenticationManager().authenticate(username, password);
            } catch (Exception e) {
                logger.warn("Failed to authenticate management API request", e);
                return null;
            }
        }

        private void sendAuthenticationRequired(ChannelHandlerContext ctx) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1,
                UNAUTHORIZED,
                Unpooled.copiedBuffer("{\"error\":\"Authentication required\"}", CharsetUtil.UTF_8)
            );
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
            response.headers().set(HttpHeaderNames.WWW_AUTHENTICATE, "Basic realm=\"AMQP Management API\"");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }

        private Object routeRequest(HttpMethod method, String uri, FullHttpRequest request) throws Exception {
            // Parse URI and route to appropriate handler
            String[] parts = uri.split("\\?")[0].split("/");

            if (parts.length < 2) {
                return Map.of("error", "Invalid URI");
            }

            String resource = parts[1];

            switch (resource) {
                case "api":
                    return handleApiRoot(method, parts);

                case "overview":
                    return managementApi.getOverview();

                case "vhosts":
                    return handleVhosts(method, parts, request);

                case "users":
                    return handleUsers(method, parts, request);

                case "exchanges":
                    return handleExchanges(method, parts);

                case "queues":
                    return handleQueues(method, parts);

                case "consumers":
                    return handleConsumers(method, parts);

                case "health":
                    return managementApi.healthCheck();

                default:
                    throw new IllegalArgumentException("Unknown resource: " + resource);
            }
        }

        private Object handleApiRoot(HttpMethod method, String[] parts) {
            return Map.of(
                "name", "AMQP Management API",
                "version", "1.0.0",
                "endpoints", List.of(
                    "/api/overview",
                    "/api/vhosts",
                    "/api/users",
                    "/api/exchanges",
                    "/api/queues",
                    "/api/consumers",
                    "/api/health"
                )
            );
        }

        private Object handleVhosts(HttpMethod method, String[] parts, FullHttpRequest request) {
            if (method == HttpMethod.GET) {
                if (parts.length == 2) {
                    return managementApi.getVirtualHosts();
                } else if (parts.length == 3) {
                    return managementApi.getVirtualHost(parts[2]);
                }
            } else if (method == HttpMethod.PUT && parts.length == 3) {
                managementApi.createVirtualHost(parts[2]);
                return Map.of("status", "created");
            } else if (method == HttpMethod.DELETE && parts.length == 3) {
                managementApi.deleteVirtualHost(parts[2]);
                return Map.of("status", "deleted");
            }

            throw new IllegalArgumentException("Invalid vhosts request");
        }

        private Object handleUsers(HttpMethod method, String[] parts, FullHttpRequest request) throws Exception {
            if (method == HttpMethod.GET) {
                if (parts.length == 2) {
                    return managementApi.getUsers();
                } else if (parts.length == 3) {
                    return managementApi.getUser(parts[2]);
                }
            } else if (method == HttpMethod.PUT && parts.length == 3) {
                String body = request.content().toString(StandardCharsets.UTF_8);
                Map<String, Object> data = objectMapper.readValue(body, Map.class);

                String username = parts[2];
                String password = (String) data.get("password");
                Set<String> tags = new HashSet<>((List<String>) data.getOrDefault("tags", List.of()));

                managementApi.createUser(username, password, tags);
                return Map.of("status", "created");
            } else if (method == HttpMethod.DELETE && parts.length == 3) {
                managementApi.deleteUser(parts[2]);
                return Map.of("status", "deleted");
            }

            throw new IllegalArgumentException("Invalid users request");
        }

        private Object handleExchanges(HttpMethod method, String[] parts) {
            if (method == HttpMethod.GET) {
                if (parts.length >= 3) {
                    String vhost = parts[2];
                    if (parts.length == 3) {
                        return managementApi.getExchanges(vhost);
                    } else if (parts.length == 4) {
                        return managementApi.getExchange(vhost, parts[3]);
                    }
                }
            }

            throw new IllegalArgumentException("Invalid exchanges request");
        }

        private Object handleQueues(HttpMethod method, String[] parts) {
            if (method == HttpMethod.GET) {
                if (parts.length >= 3) {
                    String vhost = parts[2];
                    if (parts.length == 3) {
                        return managementApi.getQueues(vhost);
                    } else if (parts.length == 4) {
                        return managementApi.getQueue(vhost, parts[3]);
                    }
                }
            }

            throw new IllegalArgumentException("Invalid queues request");
        }

        private Object handleConsumers(HttpMethod method, String[] parts) {
            if (method == HttpMethod.GET) {
                return managementApi.getConsumers();
            }

            throw new IllegalArgumentException("Invalid consumers request");
        }

        private void sendJsonResponse(ChannelHandlerContext ctx, HttpResponseStatus status, Object data) {
            try {
                String json = objectMapper.writeValueAsString(data);
                byte[] content = json.getBytes(CharsetUtil.UTF_8);

                FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, status, Unpooled.wrappedBuffer(content));

                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.length);
                // SECURITY FIX: Remove wildcard CORS - only allow specific origins if needed
                // For production, configure specific allowed origins via configuration
                // response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, configuredOrigin);

                ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            } catch (Exception e) {
                logger.error("Error sending JSON response", e);
                ctx.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Exception in HTTP handler", cause);
            ctx.close();
        }
    }
}
