package com.amqp.protocol.v10.server;

import com.amqp.protocol.v10.connection.*;
import com.amqp.protocol.v10.delivery.*;
import com.amqp.protocol.v10.frame.Amqp10Frame;
import com.amqp.protocol.v10.frame.FrameType;
import com.amqp.protocol.v10.messaging.Message10;
import com.amqp.protocol.v10.transport.*;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;
import com.amqp.protocol.v10.types.TypeEncoder;
import com.amqp.server.AmqpBroker;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP 1.0 Connection Handler.
 *
 * Handles the AMQP 1.0 protocol state machine and performative processing.
 */
public class Amqp10ConnectionHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(Amqp10ConnectionHandler.class);

    private final AmqpBroker broker;
    private final String containerId;
    private final int maxFrameSize;
    private final boolean requireSasl;

    private Amqp10Connection connection;
    private BrokerAdapter10 brokerAdapter;

    // SASL state
    private boolean saslComplete = false;
    private String authenticatedUser = null;

    public Amqp10ConnectionHandler(AmqpBroker broker, String containerId,
                                    int maxFrameSize, boolean requireSasl) {
        this.broker = broker;
        this.containerId = containerId;
        this.maxFrameSize = maxFrameSize;
        this.requireSasl = requireSasl;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.debug("New AMQP 1.0 connection from {}", ctx.channel().remoteAddress());
        connection = new Amqp10Connection(ctx.channel(), containerId);
        connection.setMaxFrameSize(maxFrameSize);
        brokerAdapter = new BrokerAdapter10(broker, connection);

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.debug("AMQP 1.0 connection closed from {}", ctx.channel().remoteAddress());

        if (connection != null) {
            connection.closeAllSessions();
        }

        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof Amqp10Frame) {
            handleFrame(ctx, (Amqp10Frame) msg);
        } else if (msg instanceof ByteBuf) {
            // Could be protocol header response
            handleProtocolEvent(ctx, msg);
        } else {
            log.warn("Unknown message type: {}", msg.getClass());
        }
    }

    private void handleProtocolEvent(ChannelHandlerContext ctx, Object msg) {
        // Protocol header handling is done in Amqp10ProtocolDecoder
        // This handles any follow-up events
    }

    private void handleFrame(ChannelHandlerContext ctx, Amqp10Frame frame) {
        try {
            int channel = frame.getChannel();
            ByteBuf body = frame.content();

            if (!body.isReadable()) {
                // Empty frame (heartbeat)
                log.trace("Received heartbeat frame");
                return;
            }

            // Decode the performative
            Object decoded = TypeDecoder.decode(body);
            if (!(decoded instanceof DescribedType)) {
                log.warn("Expected described type, got: {}", decoded);
                return;
            }

            DescribedType described = (DescribedType) decoded;
            Object descriptor = described.getDescriptor();

            if (!(descriptor instanceof Number)) {
                log.warn("Expected numeric descriptor, got: {}", descriptor);
                return;
            }

            long descriptorCode = ((Number) descriptor).longValue();

            // Handle based on frame type
            if (frame.getType() == FrameType.SASL) {
                handleSaslFrame(ctx, descriptorCode, described);
            } else {
                handleAmqpFrame(ctx, channel, descriptorCode, described, body);
            }

        } finally {
            frame.release();
        }
    }

    private void handleSaslFrame(ChannelHandlerContext ctx, long descriptorCode,
                                  DescribedType described) {
        // SASL handling will be implemented with SASL mechanisms
        log.debug("SASL frame received: 0x{}", Long.toHexString(descriptorCode));
    }

    private void handleAmqpFrame(ChannelHandlerContext ctx, int channel, long descriptorCode,
                                  DescribedType described, ByteBuf remainingBody) {
        switch ((int) descriptorCode) {
            case 0x10: // Open
                handleOpen(ctx, Open.decode(described));
                break;
            case 0x11: // Begin
                handleBegin(ctx, channel, Begin.decode(described));
                break;
            case 0x12: // Attach
                handleAttach(ctx, channel, Attach.decode(described));
                break;
            case 0x13: // Flow
                handleFlow(ctx, channel, Flow.decode(described));
                break;
            case 0x14: // Transfer
                handleTransfer(ctx, channel, Transfer.decode(described), remainingBody);
                break;
            case 0x15: // Disposition
                handleDisposition(ctx, channel, Disposition.decode(described));
                break;
            case 0x16: // Detach
                handleDetach(ctx, channel, Detach.decode(described));
                break;
            case 0x17: // End
                handleEnd(ctx, channel, End.decode(described));
                break;
            case 0x18: // Close
                handleClose(ctx, Close.decode(described));
                break;
            default:
                log.warn("Unknown AMQP performative: 0x{}", Long.toHexString(descriptorCode));
        }
    }

    private void handleOpen(ChannelHandlerContext ctx, Open open) {
        log.debug("Received Open from {}", open.getContainerId());

        connection.onOpenReceived(open);

        // Send our Open response
        Open response = connection.createOpen();
        sendPerformative(ctx, 0, response);
        connection.onOpenSent();

        log.info("AMQP 1.0 connection opened: {} <-> {}",
                connection.getContainerId(), connection.getRemoteContainerId());
    }

    private void handleBegin(ChannelHandlerContext ctx, int channel, Begin begin) {
        log.debug("Received Begin on channel {}", channel);

        // Create or find session
        Amqp10Session session;
        if (begin.getRemoteChannel() != null) {
            // Response to our Begin
            session = connection.getSession(begin.getRemoteChannel());
        } else {
            // New session from peer
            session = connection.createSession();
        }

        if (session == null) {
            log.error("No session for Begin");
            return;
        }

        session.setRemoteChannel(channel);
        session.onBeginReceived(begin);

        // Send Begin response
        Begin response = session.createBegin();
        response.setRemoteChannel(channel);
        sendPerformative(ctx, session.getLocalChannel(), response);
        session.onBeginSent();
    }

    private void handleAttach(ChannelHandlerContext ctx, int channel, Attach attach) {
        log.debug("Received Attach for link '{}' on channel {}", attach.getName(), channel);

        Amqp10Session session = connection.getSession(channel);
        if (session == null) {
            log.error("No session for channel {}", channel);
            return;
        }

        // Check if we already have this link
        Amqp10Link link = session.getLinkByName(attach.getName());
        if (link == null) {
            // Create new link (opposite role from peer)
            if (attach.isSender()) {
                // Peer is sender, we're receiver
                link = session.createReceiverLink(attach.getName(),
                        attach.getSource(), attach.getTarget());
            } else {
                // Peer is receiver, we're sender
                link = session.createSenderLink(attach.getName(),
                        attach.getSource(), attach.getTarget());
            }
        }

        link.onAttachReceived(attach);

        // Set up broker integration
        brokerAdapter.onLinkAttached(session, link);

        // Send Attach response
        Attach response = link.createAttach();
        sendPerformative(ctx, session.getLocalChannel(), response);
        link.onAttachSent();

        // For receiver links, issue initial credit
        if (link.isReceiver()) {
            ReceiverLink receiver = (ReceiverLink) link;
            receiver.setLinkCredit(receiver.getPrefetchCredit());

            Flow flow = receiver.createFlow();
            sendPerformative(ctx, session.getLocalChannel(), flow);
        }
    }

    private void handleFlow(ChannelHandlerContext ctx, int channel, Flow flow) {
        log.debug("Received Flow on channel {}", channel);

        Amqp10Session session = connection.getSession(channel);
        if (session == null) {
            return;
        }

        session.onFlowReceived(flow);

        // If echo requested, send our flow state
        if (flow.isEcho()) {
            Flow response = session.createFlow();
            if (flow.getHandle() != null) {
                Amqp10Link link = session.getLink(flow.getHandle());
                if (link != null) {
                    response = link.createFlow();
                }
            }
            sendPerformative(ctx, session.getLocalChannel(), response);
        }

        // Trigger message delivery if sender got credit
        if (flow.isLinkFlow() && flow.getLinkCredit() != null) {
            Amqp10Link link = session.getLink(flow.getHandle());
            if (link != null && link.isSender()) {
                brokerAdapter.onCreditAvailable(session, (SenderLink) link);
            }
        }
    }

    private void handleTransfer(ChannelHandlerContext ctx, int channel,
                                 Transfer transfer, ByteBuf payload) {
        log.debug("Received Transfer on channel {}, handle {}", channel, transfer.getHandle());

        Amqp10Session session = connection.getSession(channel);
        if (session == null) {
            return;
        }

        Amqp10Link link = session.getLink(transfer.getHandle());
        if (link == null || !link.isReceiver()) {
            log.warn("Transfer for unknown/invalid link: {}", transfer.getHandle());
            return;
        }

        ReceiverLink receiver = (ReceiverLink) link;

        // Set payload on transfer
        if (payload.isReadable()) {
            transfer.setPayload(payload.retainedSlice());
        }

        // Handle the transfer
        receiver.onTransfer(transfer, transfer.getPayload());

        // Process the message through broker
        if (!transfer.hasMore()) {
            ReceiverLink.ReceiverDelivery delivery = null;
            // The delivery is tracked in the link
            // BrokerAdapter will handle the message
            brokerAdapter.onMessageReceived(session, receiver, transfer);
        }
    }

    private void handleDisposition(ChannelHandlerContext ctx, int channel,
                                    Disposition disposition) {
        log.debug("Received Disposition on channel {}", channel);

        Amqp10Session session = connection.getSession(channel);
        if (session == null) {
            return;
        }

        // Handle settled deliveries
        long first = disposition.getFirst();
        long last = disposition.getLastOrFirst();

        for (long deliveryId = first; deliveryId <= last; deliveryId++) {
            // Find the link and delivery
            for (Amqp10Link link : session.getLinks().values()) {
                if (link.isSender()) {
                    SenderLink sender = (SenderLink) link;
                    sender.onDisposition(disposition);
                }
            }

            if (disposition.isSettled()) {
                session.settleDelivery(deliveryId);
            }
        }
    }

    private void handleDetach(ChannelHandlerContext ctx, int channel, Detach detach) {
        log.debug("Received Detach on channel {}, handle {}", channel, detach.getHandle());

        Amqp10Session session = connection.getSession(channel);
        if (session == null) {
            return;
        }

        Amqp10Link link = session.getLink(detach.getHandle());
        if (link == null) {
            return;
        }

        link.onDetachReceived(detach);

        // Notify broker
        brokerAdapter.onLinkDetached(session, link);

        // Send Detach response if needed
        if (link.getState() == LinkState.DETACH_RCVD) {
            Detach response = link.createDetach(detach.isClosed());
            sendPerformative(ctx, session.getLocalChannel(), response);
            link.onDetachSent();
        }
    }

    private void handleEnd(ChannelHandlerContext ctx, int channel, End end) {
        log.debug("Received End on channel {}", channel);

        Amqp10Session session = connection.getSession(channel);
        if (session == null) {
            return;
        }

        session.onEndReceived(end);

        // Send End response
        if (session.getState() == SessionState.END_RCVD) {
            End response = session.createEnd();
            sendPerformative(ctx, session.getLocalChannel(), response);
            session.onEndSent();
        }
    }

    private void handleClose(ChannelHandlerContext ctx, Close close) {
        log.debug("Received Close");

        connection.onCloseReceived(close);

        // Send Close response
        if (connection.getState() == ConnectionState.CLOSE_RCVD) {
            Close response = connection.createClose();
            sendPerformative(ctx, 0, response);
            connection.onCloseSent();
        }

        // Close the channel
        ctx.close();
    }

    private void sendPerformative(ChannelHandlerContext ctx, int channel, Performative performative) {
        ByteBuf body = ctx.alloc().buffer();
        performative.encode(body);

        Amqp10Frame frame = new Amqp10Frame(FrameType.AMQP, channel, body);
        ctx.writeAndFlush(frame);

        log.trace("Sent {} on channel {}", performative.getClass().getSimpleName(), channel);
    }

    public void sendTransfer(ChannelHandlerContext ctx, int channel, Transfer transfer) {
        ByteBuf body = ctx.alloc().buffer();

        // Encode transfer performative
        transfer.encode(body);

        // Append message payload
        ByteBuf payload = transfer.getPayload();
        if (payload != null && payload.isReadable()) {
            body.writeBytes(payload);
        }

        Amqp10Frame frame = new Amqp10Frame(FrameType.AMQP, channel, body);
        ctx.writeAndFlush(frame);
    }

    public void sendDisposition(ChannelHandlerContext ctx, int channel, Disposition disposition) {
        sendPerformative(ctx, channel, disposition);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            // Send empty frame as heartbeat
            Amqp10Frame heartbeat = new Amqp10Frame(FrameType.AMQP, 0,
                    ctx.alloc().buffer(0));
            ctx.writeAndFlush(heartbeat);
            log.trace("Sent heartbeat");
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in AMQP 1.0 connection", cause);

        // Send Close with error
        if (connection != null && connection.isOpen()) {
            ErrorCondition error = new ErrorCondition(
                    ErrorCondition.INTERNAL_ERROR,
                    cause.getMessage()
            );
            Close close = connection.createClose(error);
            sendPerformative(ctx, 0, close);
        }

        ctx.close();
    }

    public Amqp10Connection getConnection() {
        return connection;
    }
}
