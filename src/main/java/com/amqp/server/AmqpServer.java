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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpServer {
    private static final Logger logger = LoggerFactory.getLogger(AmqpServer.class);
    
    private final int port;
    private final AmqpBroker broker;
    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    
    public AmqpServer(int port, AmqpBroker broker) {
        this.port = port;
        this.broker = broker;
    }
    
    public void start() throws InterruptedException {
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
                     pipeline.addLast("protocolHeaderDecoder", new AmqpCodec.ProtocolHeaderDecoder());
                     pipeline.addLast("frameEncoder", new AmqpCodec.AmqpFrameEncoder());
                     pipeline.addLast("connectionHandler", new AmqpConnectionHandler(broker));
                 }
             });
            
            ChannelFuture f = b.bind(port).sync();
            serverChannel = f.channel();
            
            logger.info("AMQP Server started on port {}", port);
            
            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
    
    public void stop() {
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