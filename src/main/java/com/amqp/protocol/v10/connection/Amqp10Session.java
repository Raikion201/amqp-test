package com.amqp.protocol.v10.connection;

import com.amqp.protocol.v10.server.Transaction10;
import com.amqp.protocol.v10.transaction.Coordinator;
import com.amqp.protocol.v10.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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
    private final Map<Long, Long> remoteToLocalHandle = new ConcurrentHashMap<>();
    private final AtomicLong nextHandle = new AtomicLong(0);

    // Unsettled deliveries
    private final Map<Long, Delivery> unsettledDeliveries = new ConcurrentHashMap<>();
    private final AtomicLong nextDeliveryId = new AtomicLong(0);

    // Transaction tracking
    private final Map<String, Transaction10> transactions = new ConcurrentHashMap<>();
    private CoordinatorLink coordinatorLink;

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
        // Only update remoteChannel from begin if it's provided (response to our Begin)
        // If null, the remoteChannel was already set from the incoming frame's channel
        if (begin.getRemoteChannel() != null) {
            this.remoteChannel = begin.getRemoteChannel();
        }

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

    public CoordinatorLink createCoordinatorLink(String name, Source source, Coordinator coordinator) {
        long handle = nextHandle.getAndIncrement();
        CoordinatorLink link = new CoordinatorLink(this, name, handle, source, coordinator);
        links.put(handle, link);
        linksByName.put(name, link);
        this.coordinatorLink = link;
        return link;
    }

    public Amqp10Link getLink(long handle) {
        return links.get(handle);
    }

    /**
     * Get link by remote handle (maps remote handle to local handle first).
     */
    public Amqp10Link getLinkByRemoteHandle(long remoteHandle) {
        Long localHandle = remoteToLocalHandle.get(remoteHandle);
        if (localHandle != null) {
            return links.get(localHandle);
        }
        // Fallback: try direct lookup (backwards compatibility)
        return links.get(remoteHandle);
    }

    /**
     * Map a remote handle to a local handle.
     */
    public void mapRemoteHandle(long remoteHandle, long localHandle) {
        remoteToLocalHandle.put(remoteHandle, localHandle);
    }

    /**
     * Remove remote handle mapping.
     */
    public void unmapRemoteHandle(long remoteHandle) {
        remoteToLocalHandle.remove(remoteHandle);
    }

    public Amqp10Link getLinkByName(String name) {
        return linksByName.get(name);
    }

    public CoordinatorLink getCoordinatorLink() {
        return coordinatorLink;
    }

    public void removeLink(long handle) {
        Amqp10Link link = links.remove(handle);
        if (link != null) {
            linksByName.remove(link.getName());
        }
    }

    public void closeAllLinks() {
        for (Amqp10Link link : links.values()) {
            // Ensure proper cleanup for receiver links (releases ByteBufs)
            if (link instanceof ReceiverLink) {
                ((ReceiverLink) link).cleanup();
            }
            link.close(null);
        }
        links.clear();
        linksByName.clear();
        remoteToLocalHandle.clear();
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

    // Transaction management
    public void trackTransaction(Transaction10 txn) {
        transactions.put(txn.getTxnIdString(), txn);
    }

    public Transaction10 getTransaction(byte[] txnId) {
        String key = bytesToHex(txnId);
        Transaction10 txn = transactions.get(key);
        if (txn == null) {
            // Try exact byte match
            for (Transaction10 t : transactions.values()) {
                if (Arrays.equals(t.getTxnId(), txnId)) {
                    return t;
                }
            }
        }
        return txn;
    }

    public void removeTransaction(byte[] txnId) {
        String key = bytesToHex(txnId);
        transactions.remove(key);
    }

    public Map<String, Transaction10> getActiveTransactions() {
        return transactions;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
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

    /**
     * Allocate an outgoing transfer ID if capacity is available.
     * @return the transfer ID, or -1 if no capacity
     */
    public long allocateOutgoingId() {
        if (outgoingWindow <= 0) {
            return -1; // No capacity
        }
        outgoingWindow--;
        return nextOutgoingId.getAndIncrement();
    }

    /**
     * Release an outgoing ID if transfer couldn't be completed.
     */
    public void releaseOutgoingId() {
        outgoingWindow++;
    }

    public void updateIncomingId(long transferId) {
        this.nextIncomingId = transferId + 1;
        if (incomingWindow > 0) {
            incomingWindow--;
        }
    }

    public boolean hasOutgoingCapacity() {
        return outgoingWindow > 0;
    }

    /**
     * Replenish incoming window and return a Flow if needed.
     */
    public Flow replenishIncomingWindow() {
        long replenishThreshold = DEFAULT_INCOMING_WINDOW / 2;
        if (incomingWindow < replenishThreshold) {
            incomingWindow = DEFAULT_INCOMING_WINDOW;
            return createFlow();
        }
        return null;
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
