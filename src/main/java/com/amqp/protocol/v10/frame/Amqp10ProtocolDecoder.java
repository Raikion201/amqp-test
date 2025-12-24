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
 * State machine:
 * 1. INITIAL: Expect SASL or AMQP header
 * 2. SASL_IN_PROGRESS: Pass through SASL frames until SASL completes
 * 3. AWAITING_AMQP: Expect AMQP header after SASL
 * 4. COMPLETE: Remove self from pipeline
 */
public class Amqp10ProtocolDecoder extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(Amqp10ProtocolDecoder.class);

    public static final int PROTOCOL_HEADER_SIZE = 8;

    // AMQP 1.0.0 protocol header
    public static final byte[] AMQP_HEADER = {'A', 'M', 'Q', 'P', 0x00, 0x01, 0x00, 0x00};

    // SASL layer protocol header
    public static final byte[] SASL_HEADER = {'A', 'M', 'Q', 'P', 0x03, 0x01, 0x00, 0x00};

    private enum State {
        INITIAL,
        SASL_IN_PROGRESS,
        AWAITING_AMQP,
        COMPLETE
    }

    private final boolean requireSasl;
    private State state = State.INITIAL;

    public Amqp10ProtocolDecoder() {
        this(false);
    }

    public Amqp10ProtocolDecoder(boolean requireSasl) {
        this.requireSasl = requireSasl;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        logger.debug("decode() called: state={}, readable={}", state, in.readableBytes());

        switch (state) {
            case INITIAL:
            case AWAITING_AMQP:
                decodeHeader(ctx, in, out);
                break;

            case SASL_IN_PROGRESS:
                // Pass through all data to the next handler (frame decoder)
                if (in.isReadable()) {
                    out.add(in.readRetainedSlice(in.readableBytes()));
                }
                break;

            case COMPLETE:
                // Should not happen - we should be removed from pipeline
                if (in.isReadable()) {
                    out.add(in.readRetainedSlice(in.readableBytes()));
                }
                break;
        }
    }

    private void decodeHeader(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < PROTOCOL_HEADER_SIZE) {
            return;
        }

        // Peek at the header first to validate
        byte[] header = new byte[PROTOCOL_HEADER_SIZE];
        in.getBytes(in.readerIndex(), header);

        // Check if it looks like a protocol header
        if (header[0] != 'A' || header[1] != 'M' || header[2] != 'Q' || header[3] != 'P') {
            // Not a protocol header - this shouldn't happen in INITIAL/AWAITING_AMQP state
            throw new IllegalStateException("Expected AMQP protocol header, got invalid data");
        }

        // Now consume and validate
        in.readBytes(header);
        ProtocolType protocolType = validateHeader(header);

        logger.info("Received AMQP 1.0 protocol header: {}", protocolType);

        switch (protocolType) {
            case SASL:
                if (state == State.AWAITING_AMQP) {
                    // Got SASL when expecting AMQP
                    throw new IllegalStateException("Expected AMQP header after SASL, got SASL header");
                }
                handleSaslHeader(ctx, out);
                break;

            case AMQP:
                if (state == State.INITIAL && requireSasl) {
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
        // Check 'AMQP' signature (already verified in decodeHeader, but double-check)
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

        // Enter SASL in progress state - frames will be passed through
        state = State.SASL_IN_PROGRESS;
        logger.debug("Entered SASL negotiation mode");

        // Fire event for SASL negotiation
        ctx.fireUserEventTriggered(new ProtocolHeaderEvent(ProtocolType.SASL));
    }

    private void handleAmqpHeader(ChannelHandlerContext ctx, List<Object> out) {
        // Send AMQP header back
        sendAmqpHeader(ctx);

        // Mark complete and remove from pipeline
        state = State.COMPLETE;

        // Remove this decoder from the pipeline - frame decoder is already configured
        ctx.pipeline().remove(this);

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
        logger.info("Sent SASL protocol header (8 bytes)");
    }

    /**
     * Called when SASL negotiation completes successfully.
     * The next header received should be the AMQP header.
     */
    public void saslComplete() {
        state = State.AWAITING_AMQP;
        logger.info("SASL complete, now awaiting AMQP header. State={}", state);
    }

    /**
     * Get current state for debugging.
     */
    public State getState() {
        return state;
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
