package com.amqp.protocol.v10.frame;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * AMQP 1.0 Protocol Header Decoder.
 *
 * Handles the initial protocol header exchange:
 * - AMQP protocol header: 'AMQP' 0x00 0x01 0x00 0x00 (AMQP 1.0.0)
 * - SASL protocol header: 'AMQP' 0x03 0x01 0x00 0x00 (SASL layer)
 *
 * After successful header negotiation, this decoder replaces itself with
 * the appropriate frame decoder.
 */
public class Amqp10ProtocolDecoder extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(Amqp10ProtocolDecoder.class);

    public static final int PROTOCOL_HEADER_SIZE = 8;

    // AMQP 1.0.0 protocol header
    public static final byte[] AMQP_HEADER = {'A', 'M', 'Q', 'P', 0x00, 0x01, 0x00, 0x00};

    // SASL layer protocol header
    public static final byte[] SASL_HEADER = {'A', 'M', 'Q', 'P', 0x03, 0x01, 0x00, 0x00};

    private final boolean requireSasl;
    private boolean saslNegotiated = false;

    public Amqp10ProtocolDecoder() {
        this(false);
    }

    public Amqp10ProtocolDecoder(boolean requireSasl) {
        this.requireSasl = requireSasl;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < PROTOCOL_HEADER_SIZE) {
            return;
        }

        byte[] header = new byte[PROTOCOL_HEADER_SIZE];
        in.readBytes(header);

        ProtocolType protocolType = validateHeader(header);

        logger.info("Received AMQP 1.0 protocol header: {}", protocolType);

        switch (protocolType) {
            case SASL:
                handleSaslHeader(ctx, out);
                break;

            case AMQP:
                if (requireSasl && !saslNegotiated) {
                    // Client tried AMQP but we require SASL first
                    logger.warn("SASL required but client sent AMQP header");
                    sendSaslHeader(ctx);
                    return;
                }
                handleAmqpHeader(ctx, out);
                break;

            default:
                throw new IllegalStateException("Unknown protocol type");
        }
    }

    private ProtocolType validateHeader(byte[] header) {
        // Check 'AMQP' signature
        if (header[0] != 'A' || header[1] != 'M' || header[2] != 'Q' || header[3] != 'P') {
            throw new IllegalStateException("Invalid AMQP protocol signature");
        }

        // Check protocol ID
        byte protocolId = header[4];
        byte major = header[5];
        byte minor = header[6];
        byte revision = header[7];

        if (major != 1 || minor != 0 || revision != 0) {
            throw new IllegalStateException(String.format(
                "Unsupported AMQP version: %d.%d.%d", major, minor, revision));
        }

        switch (protocolId) {
            case 0x00:
                return ProtocolType.AMQP;
            case 0x03:
                return ProtocolType.SASL;
            default:
                throw new IllegalStateException("Unknown protocol ID: " + protocolId);
        }
    }

    private void handleSaslHeader(ChannelHandlerContext ctx, List<Object> out) {
        // Send SASL header back
        sendSaslHeader(ctx);

        // Fire event for SASL negotiation
        ctx.fireUserEventTriggered(new ProtocolHeaderEvent(ProtocolType.SASL));

        // Note: We don't replace the decoder yet - SASL frames will be handled
        // and then this decoder will receive another header after SASL completes
    }

    private void handleAmqpHeader(ChannelHandlerContext ctx, List<Object> out) {
        // Send AMQP header back
        sendAmqpHeader(ctx);

        // Replace this decoder with the frame decoder
        ctx.pipeline().replace(this, "amqp10FrameDecoder", new Amqp10FrameDecoder());

        // Fire event for connection start
        ctx.fireUserEventTriggered(new ProtocolHeaderEvent(ProtocolType.AMQP));
    }

    private void sendAmqpHeader(ChannelHandlerContext ctx) {
        ByteBuf header = ctx.alloc().buffer(PROTOCOL_HEADER_SIZE);
        header.writeBytes(AMQP_HEADER);
        ctx.writeAndFlush(header);
        logger.debug("Sent AMQP 1.0 protocol header");
    }

    private void sendSaslHeader(ChannelHandlerContext ctx) {
        ByteBuf header = ctx.alloc().buffer(PROTOCOL_HEADER_SIZE);
        header.writeBytes(SASL_HEADER);
        ctx.writeAndFlush(header);
        logger.debug("Sent SASL protocol header");
    }

    /**
     * Called when SASL negotiation completes successfully.
     * The next header received should be the AMQP header.
     */
    public void saslComplete() {
        this.saslNegotiated = true;
    }

    /**
     * Protocol types for header negotiation.
     */
    public enum ProtocolType {
        AMQP,
        SASL
    }

    /**
     * Event fired when a protocol header is received.
     */
    public static class ProtocolHeaderEvent {
        private final ProtocolType type;

        public ProtocolHeaderEvent(ProtocolType type) {
            this.type = type;
        }

        public ProtocolType getType() {
            return type;
        }
    }
}
