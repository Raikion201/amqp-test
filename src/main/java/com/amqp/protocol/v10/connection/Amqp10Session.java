package com.amqp.protocol.v10.connection;

import com.amqp.protocol.v10.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AMQP 1.0 Session.
 *
 * Manages session state, flow control windows, and links.
 */
public class Amqp10Session {

    private static final Logger log = LoggerFactory.getLogger(Amqp10Session.class);

    // Default session parameters
    public static final long DEFAULT_INCOMING_WINDOW = 2048;
    public static final long DEFAULT_OUTGOING_WINDOW = 2048;
    public static final long DEFAULT_HANDLE_MAX = 0xFFFFFFFFL;

    private final Amqp10Connection connection;
    private final int localChannel;
    private Integer remoteChannel;

    private volatile SessionState state = SessionState.UNMAPPED;

    // Flow control
    private long incomingWindow = DEFAULT_INCOMING_WINDOW;
    private long outgoingWindow = DEFAULT_OUTGOING_WINDOW;
    private long handleMax = DEFAULT_HANDLE_MAX;

    // Transfer IDs
    private final AtomicLong nextOutgoingId = new AtomicLong(0);
    private long nextIncomingId = 0;

    // Links indexed by handle
    private final Map<Long, Amqp10Link> links = new ConcurrentHashMap<>();
    private final Map<String, Amqp10Link> linksByName = new ConcurrentHashMap<>();
    private final AtomicLong nextHandle = new AtomicLong(0);

    // Unsettled deliveries
    private final Map<Long, Delivery> unsettledDeliveries = new ConcurrentHashMap<>();
    private final AtomicLong nextDeliveryId = new AtomicLong(0);

    // Error tracking
    private ErrorCondition error;

    public Amqp10Session(Amqp10Connection connection, int localChannel) {
        this.connection = connection;
        this.localChannel = localChannel;
    }

    // State management
    public SessionState getState() {
        return state;
    }

    public void setState(SessionState state) {
        log.debug("Session {} state change: {} -> {}", localChannel, this.state, state);
        this.state = state;
    }

    public boolean isMapped() {
        return state == SessionState.MAPPED;
    }

    public boolean isEnding() {
        return state == SessionState.END_SENT || state == SessionState.END_RCVD;
    }

    // Begin handling
    public void onBeginReceived(Begin begin) {
        this.remoteChannel = begin.getRemoteChannel();

        // Negotiate windows
        this.nextIncomingId = begin.getNextOutgoingId();

        if (begin.getHandleMax() < handleMax) {
            this.handleMax = begin.getHandleMax();
        }

        switch (state) {
            case UNMAPPED:
                setState(SessionState.BEGIN_RCVD);
                break;
            case BEGIN_SENT:
                setState(SessionState.MAPPED);
                break;
            default:
                log.warn("Unexpected Begin in state {}", state);
        }
    }

    public void onBeginSent() {
        switch (state) {
            case UNMAPPED:
                setState(SessionState.BEGIN_SENT);
                break;
            case BEGIN_RCVD:
                setState(SessionState.MAPPED);
                break;
            default:
                log.warn("Unexpected Begin sent in state {}", state);
        }
    }

    // End handling
    public void onEndReceived(End end) {
        if (end.getError() != null) {
            this.error = end.getError();
            log.warn("Session ended by peer with error: {}", error);
        }

        switch (state) {
            case MAPPED:
                setState(SessionState.END_RCVD);
                break;
            case END_SENT:
                setState(SessionState.DISCARDING);
                connection.removeSession(localChannel);
                break;
            default:
                setState(SessionState.DISCARDING);
                connection.removeSession(localChannel);
        }
    }

    public void onEndSent() {
        switch (state) {
            case MAPPED:
                setState(SessionState.END_SENT);
                break;
            case END_RCVD:
                setState(SessionState.DISCARDING);
                connection.removeSession(localChannel);
                break;
            default:
                setState(SessionState.DISCARDING);
        }
    }

    // Flow control
    public void onFlowReceived(Flow flow) {
        // Update session-level flow state
        if (flow.getNextIncomingId() != null) {
            // Peer has received up to this transfer ID
            long peerNextIncoming = flow.getNextIncomingId();
            // Calculate available outgoing window
            this.outgoingWindow = peerNextIncoming + flow.getIncomingWindow() - nextOutgoingId.get();
        }

        // If it's a link flow, forward to the link
        if (flow.isLinkFlow()) {
            Amqp10Link link = links.get(flow.getHandle());
            if (link != null) {
                link.onFlowReceived(flow);
            } else {
                log.warn("Flow received for unknown handle: {}", flow.getHandle());
            }
        }
    }

    // Link management
    public Amqp10Link createSenderLink(String name, Source source, Target target) {
        long handle = nextHandle.getAndIncrement();
        SenderLink link = new SenderLink(this, name, handle, source, target);
        links.put(handle, link);
        linksByName.put(name, link);
        return link;
    }

    public Amqp10Link createReceiverLink(String name, Source source, Target target) {
        long handle = nextHandle.getAndIncrement();
        ReceiverLink link = new ReceiverLink(this, name, handle, source, target);
        links.put(handle, link);
        linksByName.put(name, link);
        return link;
    }

    public Amqp10Link getLink(long handle) {
        return links.get(handle);
    }

    public Amqp10Link getLinkByName(String name) {
        return linksByName.get(name);
    }

    public void removeLink(long handle) {
        Amqp10Link link = links.remove(handle);
        if (link != null) {
            linksByName.remove(link.getName());
        }
    }

    public void closeAllLinks() {
        for (Amqp10Link link : links.values()) {
            link.close(null);
        }
        links.clear();
        linksByName.clear();
    }

    // Delivery management
    public long nextDeliveryId() {
        return nextDeliveryId.getAndIncrement();
    }

    public void trackDelivery(long deliveryId, Delivery delivery) {
        unsettledDeliveries.put(deliveryId, delivery);
    }

    public void settleDelivery(long deliveryId) {
        unsettledDeliveries.remove(deliveryId);
    }

    public Delivery getDelivery(long deliveryId) {
        return unsettledDeliveries.get(deliveryId);
    }

    // Create performatives
    public Begin createBegin() {
        Begin begin = new Begin(nextOutgoingId.get(), incomingWindow, outgoingWindow);
        begin.setHandleMax(handleMax);
        if (remoteChannel != null) {
            begin.setRemoteChannel(remoteChannel);
        }
        return begin;
    }

    public End createEnd() {
        return error != null ? new End(error) : new End();
    }

    public End createEnd(ErrorCondition error) {
        this.error = error;
        return new End(error);
    }

    public Flow createFlow() {
        Flow flow = new Flow(incomingWindow, nextOutgoingId.get(), outgoingWindow);
        flow.setNextIncomingId(nextIncomingId);
        return flow;
    }

    public void close(ErrorCondition error) {
        if (state != SessionState.DISCARDING && state != SessionState.END_SENT) {
            closeAllLinks();
            this.error = error;
            // The handler should send End
        }
    }

    // Transfer tracking
    public long getNextOutgoingId() {
        return nextOutgoingId.get();
    }

    public long allocateOutgoingId() {
        return nextOutgoingId.getAndIncrement();
    }

    public void updateIncomingId(long transferId) {
        this.nextIncomingId = transferId + 1;
        this.incomingWindow--;
    }

    public boolean hasOutgoingCapacity() {
        return outgoingWindow > 0;
    }

    // Getters
    public Amqp10Connection getConnection() {
        return connection;
    }

    public int getLocalChannel() {
        return localChannel;
    }

    public Integer getRemoteChannel() {
        return remoteChannel;
    }

    public long getIncomingWindow() {
        return incomingWindow;
    }

    public long getOutgoingWindow() {
        return outgoingWindow;
    }

    public long getHandleMax() {
        return handleMax;
    }

    public ErrorCondition getError() {
        return error;
    }

    public Map<Long, Amqp10Link> getLinks() {
        return links;
    }

    // Setters
    public void setRemoteChannel(Integer remoteChannel) {
        this.remoteChannel = remoteChannel;
    }

    public void setIncomingWindow(long incomingWindow) {
        this.incomingWindow = incomingWindow;
    }

    public void setOutgoingWindow(long outgoingWindow) {
        this.outgoingWindow = outgoingWindow;
    }

    @Override
    public String toString() {
        return String.format("Amqp10Session{localChannel=%d, remoteChannel=%s, state=%s, links=%d}",
                localChannel, remoteChannel, state, links.size());
    }
}
