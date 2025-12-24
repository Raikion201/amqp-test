package com.amqp.security.sasl.amqp10;

import com.amqp.protocol.v10.frame.Amqp10Frame;
import com.amqp.protocol.v10.frame.Amqp10ProtocolDecoder;
import com.amqp.protocol.v10.frame.FrameType;
import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;
import com.amqp.protocol.v10.types.TypeEncoder;
import com.amqp.security.sasl.SaslContext;
import com.amqp.security.sasl.SaslMechanism;
import com.amqp.security.sasl.SaslNegotiator;
import com.amqp.security.tls.MutualTlsHandler;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.cert.X509Certificate;
import java.util.List;

/**
 * Handler for AMQP 1.0 SASL authentication layer.
 *
 * This handler processes SASL frames before normal AMQP frames.
 * After successful authentication, it notifies the protocol decoder
 * and passes through subsequent AMQP frames.
 */
public class Sasl10Handler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(Sasl10Handler.class);

    public static final AttributeKey<String> AUTH_USER_KEY =
            AttributeKey.valueOf("sasl.authenticated.user");

    private final SaslNegotiator negotiator;
    private final boolean required;

    private SaslContext saslContext;
    private boolean saslComplete = false;
    private boolean saslStarted = false;

    public Sasl10Handler(SaslNegotiator negotiator, boolean required) {
        this.negotiator = negotiator;
        this.required = required;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // Initialize SASL context
        saslContext = new SaslContext(ctx);

        // Check for TLS and client certificate
        if (MutualTlsHandler.isTlsAuthenticated(ctx)) {
            saslContext.setTlsEnabled(true);
            X509Certificate cert = MutualTlsHandler.getClientCertificate(ctx);
            if (cert != null) {
                saslContext.setClientCertificate(cert);
                saslContext.setTlsUsername(MutualTlsHandler.getClientCertificateUser(ctx));
            }
        }

        super.channelActive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof Amqp10ProtocolDecoder.ProtocolHeaderEvent) {
            Amqp10ProtocolDecoder.ProtocolHeaderEvent headerEvent =
                    (Amqp10ProtocolDecoder.ProtocolHeaderEvent) evt;

            if (headerEvent.getType() == Amqp10ProtocolDecoder.ProtocolType.SASL) {
                // Protocol decoder received SASL header - send mechanisms
                log.info("Received SASL header event, sending mechanisms");
                saslStarted = true;
                sendSaslMechanisms(ctx);
                return;
            }
        }

        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (saslComplete) {
            // SASL is complete, pass through
            super.channelRead(ctx, msg);
            return;
        }

        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            // Check for SASL protocol header (legacy direct handling)
            if (isSaslHeader(buf)) {
                handleSaslHeader(ctx, buf);
                return;
            }
            // Otherwise pass through (could be AMQP header after SASL)
        }

        if (msg instanceof Amqp10Frame) {
            Amqp10Frame frame = (Amqp10Frame) msg;
            if (frame.getType() == FrameType.SASL) {
                handleSaslFrame(ctx, frame);
                return;
            }
        }

        // Not a SASL message
        if (required && !saslComplete && saslStarted) {
            log.warn("SASL required but received non-SASL message during negotiation");
            ctx.close();
            return;
        }

        super.channelRead(ctx, msg);
    }

    private boolean isSaslHeader(ByteBuf buf) {
        if (buf.readableBytes() < 8) {
            return false;
        }
        return buf.getByte(buf.readerIndex()) == 'A'
                && buf.getByte(buf.readerIndex() + 1) == 'M'
                && buf.getByte(buf.readerIndex() + 2) == 'Q'
                && buf.getByte(buf.readerIndex() + 3) == 'P'
                && buf.getByte(buf.readerIndex() + 4) == 0x03; // SASL
    }

    private void handleSaslHeader(ChannelHandlerContext ctx, ByteBuf buf) {
        // Skip the header
        buf.skipBytes(8);

        log.debug("Received SASL protocol header directly");
        saslStarted = true;

        // Send our SASL header back
        ByteBuf response = ctx.alloc().buffer(8);
        response.writeBytes(Amqp10ProtocolDecoder.SASL_HEADER);
        ctx.writeAndFlush(response);

        // Send SASL mechanisms
        sendSaslMechanisms(ctx);
    }

    private void sendSaslMechanisms(ChannelHandlerContext ctx) {
        List<String> mechanisms = negotiator.getApplicableMechanisms(saslContext);

        if (mechanisms.isEmpty()) {
            log.warn("No applicable SASL mechanisms available! TLS enabled: {}, context: {}",
                    saslContext.isTlsEnabled(), saslContext);
        }

        SaslMechanismsFrame mechanismsFrame = new SaslMechanismsFrame(mechanisms);
        sendSaslFrame(ctx, mechanismsFrame);

        log.info("Sent SASL mechanisms: {}", mechanisms);
    }

    private void handleSaslFrame(ChannelHandlerContext ctx, Amqp10Frame frame) {
        try {
            ByteBuf body = frame.content();
            if (!body.isReadable()) {
                return;
            }

            Object decoded = TypeDecoder.decode(body);
            if (!(decoded instanceof DescribedType)) {
                log.warn("Expected described type in SASL frame");
                return;
            }

            DescribedType described = (DescribedType) decoded;
            Object descriptor = described.getDescriptor();
            if (!(descriptor instanceof Number)) {
                return;
            }

            long code = ((Number) descriptor).longValue();

            if (code == AmqpType.Descriptor.SASL_INIT) {
                handleSaslInit(ctx, SaslInitFrame.decode(described));
            } else if (code == AmqpType.Descriptor.SASL_RESPONSE) {
                handleSaslResponse(ctx, SaslResponseFrame.decode(described));
            } else {
                log.warn("Unexpected SASL frame: 0x{}", Long.toHexString(code));
            }

        } finally {
            frame.release();
        }
    }

    private void handleSaslInit(ChannelHandlerContext ctx, SaslInitFrame init) {
        log.info("Received SASL init: mechanism={}", init.getMechanismName());

        SaslMechanism.SaslResult result = negotiator.handleInit(
                saslContext, init.getMechanismName(), init.getInitialResponse());

        log.info("SASL result: complete={}, success={}, username={}",
                result.isComplete(), result.isSuccess(),
                result.isComplete() && result.isSuccess() ? result.getUsername() : "N/A");

        processSaslResult(ctx, result);
    }

    private void handleSaslResponse(ChannelHandlerContext ctx, SaslResponseFrame response) {
        log.debug("Received SASL response");

        SaslMechanism.SaslResult result = negotiator.handleResponse(
                saslContext, response.getResponse());

        processSaslResult(ctx, result);
    }

    private void processSaslResult(ChannelHandlerContext ctx, SaslMechanism.SaslResult result) {
        if (result.isComplete()) {
            if (result.isSuccess()) {
                // Authentication successful
                String username = result.getUsername();
                saslContext.setAuthenticatedUser(username);
                ctx.channel().attr(AUTH_USER_KEY).set(username);

                log.info("SASL authentication successful: user={}", username);

                // Send success outcome
                sendSaslFrame(ctx, SaslOutcomeFrame.success());

                // Mark as complete
                saslComplete = true;

                // Notify protocol decoder to expect AMQP header
                notifyProtocolDecoder(ctx);

            } else {
                // Authentication failed
                log.warn("SASL authentication failed: {}", result.getErrorMessage());

                int outcomeCode;
                switch (result.getOutcome()) {
                    case AUTH:
                        outcomeCode = SaslOutcomeFrame.AUTH;
                        break;
                    case SYS_PERM:
                        outcomeCode = SaslOutcomeFrame.SYS_PERM;
                        break;
                    case SYS_TEMP:
                        outcomeCode = SaslOutcomeFrame.SYS_TEMP;
                        break;
                    default:
                        outcomeCode = SaslOutcomeFrame.SYS;
                }

                sendSaslFrame(ctx, new SaslOutcomeFrame(outcomeCode));

                // Close connection on auth failure
                ctx.close();
            }
        } else {
            // Need another round (challenge/response)
            byte[] challenge = result.getChallenge();
            if (challenge != null) {
                sendSaslFrame(ctx, new SaslChallengeFrame(challenge));
            }
        }
    }

    private void notifyProtocolDecoder(ChannelHandlerContext ctx) {
        // Find the protocol decoder and notify it that SASL is complete
        ChannelHandler handler = ctx.pipeline().get("protocol");
        if (handler instanceof Amqp10ProtocolDecoder) {
            Amqp10ProtocolDecoder decoder = (Amqp10ProtocolDecoder) handler;
            decoder.saslComplete();
            log.debug("Notified protocol decoder that SASL is complete");
        } else {
            log.debug("Protocol decoder not found in pipeline (may have been removed)");
        }
    }

    private void sendSaslFrame(ChannelHandlerContext ctx, SaslPerformative performative) {
        ByteBuf body = ctx.alloc().buffer();
        TypeEncoder.encode(performative.toDescribed(), body);

        // Debug: print hex bytes of the body
        if (log.isDebugEnabled()) {
            StringBuilder hex = new StringBuilder();
            for (int i = body.readerIndex(); i < body.writerIndex(); i++) {
                hex.append(String.format("%02x ", body.getByte(i)));
            }
            log.debug("SASL frame body hex: {}", hex.toString());
        }

        Amqp10Frame frame = new Amqp10Frame(FrameType.SASL, 0, body);
        ctx.writeAndFlush(frame);

        log.info("Sent SASL frame: {} ({} bytes)", performative.getClass().getSimpleName(), frame.getSize());
    }

    public boolean isSaslComplete() {
        return saslComplete;
    }

    public SaslContext getSaslContext() {
        return saslContext;
    }

    public static String getAuthenticatedUser(ChannelHandlerContext ctx) {
        return ctx.channel().attr(AUTH_USER_KEY).get();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception in SASL handler", cause);
        ctx.close();
    }
}
