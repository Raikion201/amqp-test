package com.amqp.protocol.v10.connection;

import com.amqp.protocol.v10.delivery.*;
import com.amqp.protocol.v10.messaging.Message10;
import com.amqp.protocol.v10.server.Transaction10;
import com.amqp.protocol.v10.transaction.TransactionalState;
import com.amqp.protocol.v10.transport.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * AMQP 1.0 Receiver Link.
 *
 * Used to receive messages from a remote sender.
 */
public class ReceiverLink extends Amqp10Link {

    private static final Logger log = LoggerFactory.getLogger(ReceiverLink.class);

    // Unsettled deliveries (by delivery ID)
    private final Map<Long, ReceiverDelivery> unsettled = new ConcurrentHashMap<>();

    // Partial transfer being assembled (for multi-frame messages)
    private ReceiverDelivery currentDelivery;

    // Last completed delivery (for multi-frame messages)
    private ReceiverDelivery lastCompletedDelivery;

    // Message handler
    private Consumer<ReceiverDelivery> messageHandler;

    // Prefetch credit - configurable
    public static final long DEFAULT_PREFETCH_CREDIT = 100;
    private long prefetchCredit = DEFAULT_PREFETCH_CREDIT;

    // Security limits - configurable
    public static final long DEFAULT_MAX_MESSAGE_SIZE = 64 * 1024 * 1024; // 64 MB default
    public static final long DEFAULT_MAX_ASSEMBLY_TIME_MS = 30000; // 30 seconds default

    private long maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
    private long maxAssemblyTimeMs = DEFAULT_MAX_ASSEMBLY_TIME_MS;
    private long currentDeliveryStartTime;

    // Sender's delivery count (updated from Flow)
    private long senderDeliveryCount = 0;

    public ReceiverLink(Amqp10Session session, String name, long handle,
                        Source source, Target target) {
        super(session, name, handle, true, source, target); // role = true = receiver
    }

    /**
     * Issue link credit to the sender.
     */
    public void flow(long credit) {
        this.linkCredit = credit;
        // Handler will send Flow performative
    }

    /**
     * Handle an incoming transfer.
     *
     * @return null if accepted, or an ErrorCondition if rejected
     */
    public ErrorCondition onTransfer(Transfer transfer, ByteBuf payload) {
        // Check for aborted transfer
        if (transfer.isAborted()) {
            if (currentDelivery != null) {
                log.debug("Transfer aborted for delivery {}", currentDelivery.getDeliveryId());
                currentDelivery.release();
                currentDelivery = null;
            }
            return null;
        }

        if (transfer.getDeliveryId() != null && currentDelivery == null) {
            // Start of a new delivery
            currentDelivery = new ReceiverDelivery(
                    transfer.getDeliveryId(),
                    transfer.getDeliveryTag(),
                    transfer.isSettled()
            );
            currentDeliveryStartTime = System.currentTimeMillis();
        } else if (currentDelivery != null) {
            // Check multi-frame assembly timeout
            long elapsed = System.currentTimeMillis() - currentDeliveryStartTime;
            if (elapsed > maxAssemblyTimeMs) {
                log.warn("Multi-frame message assembly timeout after {}ms for delivery {}",
                        elapsed, currentDelivery.getDeliveryId());
                currentDelivery.release();
                currentDelivery = null;
                return new ErrorCondition(
                        ErrorCondition.RESOURCE_LIMIT_EXCEEDED,
                        "Message assembly timeout exceeded"
                );
            }
        }

        // Add payload to current delivery
        if (payload != null && payload.isReadable()) {
            // Check message size limit
            long currentSize = currentDelivery.getPayloadSize();
            long newSize = currentSize + payload.readableBytes();
            if (newSize > maxMessageSize) {
                log.warn("Message size {} exceeds limit {} for delivery {}",
                        newSize, maxMessageSize, currentDelivery.getDeliveryId());
                currentDelivery.release();
                currentDelivery = null;
                return new ErrorCondition(
                        ErrorCondition.MESSAGE_SIZE_EXCEEDED,
                        "Message size " + newSize + " exceeds limit " + maxMessageSize
                );
            }
            currentDelivery.addPayload(payload);
        }

        // Check if transfer is complete
        if (!transfer.hasMore()) {
            // Complete the delivery
            ByteBuf completePayload = currentDelivery.getPayload();
            Message10 message = Message10.decode(completePayload);
            currentDelivery.setMessage(message);

            // Track unsettled if not pre-settled
            if (!currentDelivery.isRemoteSettled()) {
                unsettled.put(currentDelivery.getDeliveryId(), currentDelivery);
            }

            // Update link credit (decrease available credit)
            // Note: deliveryCount is maintained by sender, we track senderDeliveryCount from Flow
            if (linkCredit > 0) {
                linkCredit--;
            }

            // Store as last completed delivery before clearing (for multi-frame message access)
            lastCompletedDelivery = currentDelivery;

            // Deliver to handler
            if (messageHandler != null) {
                try {
                    messageHandler.accept(currentDelivery);
                } catch (Exception e) {
                    log.error("Error in message handler", e);
                }
            }

            currentDelivery = null;
        }

        return null;
    }

    /**
     * Override to track sender's delivery count from flow.
     */
    @Override
    public void onFlowReceived(Flow flow) {
        super.onFlowReceived(flow);
        if (flow.getDeliveryCount() != null) {
            this.senderDeliveryCount = flow.getDeliveryCount();
        }
    }

    /**
     * Get the sender's delivery count as reported in Flow.
     */
    public long getSenderDeliveryCount() {
        return senderDeliveryCount;
    }

    /**
     * Check and cleanup any timed-out partial deliveries.
     *
     * @return ErrorCondition if a delivery was timed out, null otherwise
     */
    public ErrorCondition checkPartialDeliveryTimeout() {
        if (currentDelivery != null) {
            long elapsed = System.currentTimeMillis() - currentDeliveryStartTime;
            if (elapsed > maxAssemblyTimeMs) {
                log.warn("Cleaning up timed-out partial delivery {} after {}ms",
                        currentDelivery.getDeliveryId(), elapsed);
                currentDelivery.release();
                currentDelivery = null;
                return new ErrorCondition(
                        ErrorCondition.RESOURCE_LIMIT_EXCEEDED,
                        "Partial message assembly timed out"
                );
            }
        }
        return null;
    }

    /**
     * Accept a delivery.
     */
    public void accept(long deliveryId) {
        disposition(deliveryId, Accepted.INSTANCE, true);
    }

    /**
     * Accept a delivery within a transaction.
     */
    public void acceptTransactional(long deliveryId, byte[] txnId) {
        TransactionalState txnState = new TransactionalState(txnId, Accepted.INSTANCE);
        disposition(deliveryId, txnState, false); // Not settled until txn commits

        // Track in transaction
        Transaction10 txn = session.getTransaction(txnId);
        if (txn != null) {
            ReceiverDelivery delivery = unsettled.get(deliveryId);
            if (delivery != null) {
                txn.addAcknowledgment(null, deliveryId);
            }
        }
    }

    /**
     * Reject a delivery.
     */
    public void reject(long deliveryId, ErrorCondition error) {
        Rejected rejected = new Rejected();
        if (error != null) {
            rejected.setError(error);
        }
        disposition(deliveryId, rejected, true);
    }

    /**
     * Release a delivery for redelivery.
     */
    public void release(long deliveryId) {
        disposition(deliveryId, Released.INSTANCE, true);
    }

    /**
     * Modify a delivery.
     */
    public void modify(long deliveryId, boolean deliveryFailed, boolean undeliverableHere) {
        Modified modified = new Modified()
                .setDeliveryFailed(deliveryFailed)
                .setUndeliverableHere(undeliverableHere);
        disposition(deliveryId, modified, true);
    }

    /**
     * Send a disposition for a delivery.
     */
    public void disposition(long deliveryId, DeliveryState state, boolean settled) {
        ReceiverDelivery delivery = unsettled.get(deliveryId);
        if (delivery != null) {
            delivery.setLocalState(state);
            delivery.setLocalSettled(settled);

            if (settled) {
                unsettled.remove(deliveryId);
            }
        }

        // Create disposition performative (handler will send it)
    }

    /**
     * Create a disposition performative for a delivery.
     */
    public Disposition createDisposition(long deliveryId, DeliveryState state, boolean settled) {
        Disposition disp = new Disposition(true, deliveryId); // role = true = receiver
        disp.setState(state instanceof DeliveryState ? ((DeliveryState) state).toDescribed() : state);
        disp.setSettled(settled);
        return disp;
    }

    /**
     * Get number of unsettled deliveries.
     */
    public int getUnsettledCount() {
        return unsettled.size();
    }

    /**
     * Set the message handler.
     */
    public void setMessageHandler(Consumer<ReceiverDelivery> handler) {
        this.messageHandler = handler;
    }

    public long getPrefetchCredit() {
        return prefetchCredit;
    }

    public void setPrefetchCredit(long prefetchCredit) {
        this.prefetchCredit = prefetchCredit;
    }

    public long getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(long maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public long getMaxAssemblyTimeMs() {
        return maxAssemblyTimeMs;
    }

    public void setMaxAssemblyTimeMs(long maxAssemblyTimeMs) {
        this.maxAssemblyTimeMs = maxAssemblyTimeMs;
    }

    /**
     * Check if there is a partial delivery in progress.
     */
    public boolean hasPartialDelivery() {
        return currentDelivery != null;
    }

    /**
     * Get the last completed delivery.
     * This is used for multi-frame messages to get the complete assembled message.
     */
    public ReceiverDelivery getLastCompletedDelivery() {
        return lastCompletedDelivery;
    }

    /**
     * Cleanup all resources when link is closed.
     * CRITICAL: Must be called on link close to prevent ByteBuf leaks!
     */
    public void cleanup() {
        // Release partial delivery if any
        if (currentDelivery != null) {
            try {
                log.debug("Releasing partial delivery {} on link cleanup", currentDelivery.getDeliveryId());
                currentDelivery.release();
            } catch (Exception e) {
                log.warn("Error releasing partial delivery during cleanup", e);
            }
            currentDelivery = null;
        }

        // Release all unsettled deliveries
        for (ReceiverDelivery delivery : unsettled.values()) {
            try {
                delivery.release();
            } catch (Exception e) {
                log.warn("Error releasing unsettled delivery {} during cleanup", delivery.getDeliveryId(), e);
            }
        }
        unsettled.clear();
        log.debug("ReceiverLink {} cleaned up", name);
    }

    /**
     * Close the link with proper resource cleanup.
     */
    @Override
    public void close(ErrorCondition error) {
        cleanup();
        super.close(error);
    }

    /**
     * Represents a delivery received on this link.
     */
    public static class ReceiverDelivery {
        private final long deliveryId;
        private final byte[] deliveryTag;
        private final boolean remoteSettled;

        private CompositeByteBuf payloadBuffer;
        private Message10 message;
        private DeliveryState localState;
        private boolean localSettled;

        public ReceiverDelivery(long deliveryId, byte[] deliveryTag, boolean remoteSettled) {
            this.deliveryId = deliveryId;
            this.deliveryTag = deliveryTag;
            this.remoteSettled = remoteSettled;
            this.payloadBuffer = Unpooled.compositeBuffer();
        }

        public void addPayload(ByteBuf payload) {
            payloadBuffer.addComponent(true, payload.retainedSlice());
        }

        public ByteBuf getPayload() {
            return payloadBuffer;
        }

        public long getPayloadSize() {
            return payloadBuffer != null ? payloadBuffer.readableBytes() : 0;
        }

        public long getDeliveryId() {
            return deliveryId;
        }

        public byte[] getDeliveryTag() {
            return deliveryTag;
        }

        public boolean isRemoteSettled() {
            return remoteSettled;
        }

        public Message10 getMessage() {
            return message;
        }

        public void setMessage(Message10 message) {
            this.message = message;
        }

        public DeliveryState getLocalState() {
            return localState;
        }

        public void setLocalState(DeliveryState localState) {
            this.localState = localState;
        }

        public boolean isLocalSettled() {
            return localSettled;
        }

        public void setLocalSettled(boolean localSettled) {
            this.localSettled = localSettled;
        }

        public boolean isFullySettled() {
            return remoteSettled && localSettled;
        }

        public void release() {
            if (payloadBuffer != null) {
                payloadBuffer.release();
                payloadBuffer = null;
            }
        }
    }
}
