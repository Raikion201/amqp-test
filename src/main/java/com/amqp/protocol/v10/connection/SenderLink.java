package com.amqp.protocol.v10.connection;

import com.amqp.protocol.v10.delivery.DeliveryState;
import com.amqp.protocol.v10.messaging.Message10;
import com.amqp.protocol.v10.server.Transaction10;
import com.amqp.protocol.v10.transaction.TransactionalState;
import com.amqp.protocol.v10.transport.*;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AMQP 1.0 Sender Link.
 *
 * Used to send messages to a remote receiver.
 */
public class SenderLink extends Amqp10Link {

    private static final Logger log = LoggerFactory.getLogger(SenderLink.class);

    // Unsettled deliveries (by delivery tag)
    private final Map<Long, SenderDelivery> unsettled = new ConcurrentHashMap<>();
    private final AtomicLong nextDeliveryTag = new AtomicLong(0);

    public SenderLink(Amqp10Session session, String name, long handle,
                      Source source, Target target) {
        super(session, name, handle, false, source, target); // role = false = sender
    }

    /**
     * Send a message on this link.
     *
     * @param message the message to send
     * @param settled whether to pre-settle the delivery
     * @return the delivery for tracking, or null if no credit
     */
    public SenderDelivery send(Message10 message, boolean settled) {
        if (!isAttached()) {
            throw new IllegalStateException("Link not attached");
        }

        if (!hasCredit()) {
            log.debug("No link credit available on link {}", name);
            return null;
        }

        // Allocate delivery
        long deliveryId = session.nextDeliveryId();
        long deliveryTag = nextDeliveryTag.getAndIncrement();
        byte[] tag = longToBytes(deliveryTag);

        SenderDelivery delivery = new SenderDelivery(deliveryId, tag, message, settled);

        if (!settled) {
            unsettled.put(deliveryTag, delivery);
        }

        // Create transfer
        Transfer transfer = new Transfer(handle);
        transfer.setDeliveryId(deliveryId);
        transfer.setDeliveryTag(tag);
        transfer.setMessageFormat(Transfer.MESSAGE_FORMAT_AMQP);
        transfer.setSettled(settled);

        // Encode message
        ByteBuf payload = message.encode();
        transfer.setPayload(payload);

        // Consume credit and update delivery count
        consumeCredit();
        deliveryCount++;

        // Store delivery for later reference
        delivery.setTransfer(transfer);

        return delivery;
    }

    /**
     * Send a message within a transaction.
     *
     * @param message the message to send
     * @param txnId the transaction ID
     * @return the delivery for tracking, or null if no credit
     */
    public SenderDelivery sendTransactional(Message10 message, byte[] txnId) {
        if (!isAttached()) {
            throw new IllegalStateException("Link not attached");
        }

        if (!hasCredit()) {
            log.debug("No link credit available on link {}", name);
            return null;
        }

        // Allocate delivery
        long deliveryId = session.nextDeliveryId();
        long deliveryTag = nextDeliveryTag.getAndIncrement();
        byte[] tag = longToBytes(deliveryTag);

        SenderDelivery delivery = new SenderDelivery(deliveryId, tag, message, false);
        delivery.setTxnId(txnId);
        unsettled.put(deliveryTag, delivery);

        // Create transfer with transactional state
        Transfer transfer = new Transfer(handle);
        transfer.setDeliveryId(deliveryId);
        transfer.setDeliveryTag(tag);
        transfer.setMessageFormat(Transfer.MESSAGE_FORMAT_AMQP);
        transfer.setSettled(false); // Transactional sends are never pre-settled

        // Set transactional state on the transfer
        TransactionalState txnState = new TransactionalState(txnId);
        transfer.setState(txnState.toDescribed());

        // Encode message
        ByteBuf payload = message.encode();
        transfer.setPayload(payload);

        // Consume credit and update delivery count
        consumeCredit();
        deliveryCount++;

        // Store delivery for later reference
        delivery.setTransfer(transfer);

        // Track in transaction
        Transaction10 txn = session.getTransaction(txnId);
        if (txn != null) {
            txn.addPublish(null, target.getAddress(), payload.array());
        }

        return delivery;
    }

    /**
     * Handle disposition from the receiver.
     * Returns list of affected deliveries for further processing (e.g., Released handling).
     */
    public java.util.List<SenderDelivery> onDisposition(Disposition disposition) {
        long first = disposition.getFirst();
        long last = disposition.getLastOrFirst();
        java.util.List<SenderDelivery> affectedDeliveries = new java.util.ArrayList<>();

        for (long deliveryId = first; deliveryId <= last; deliveryId++) {
            // Find the delivery by ID
            for (SenderDelivery delivery : unsettled.values()) {
                if (delivery.getDeliveryId() == deliveryId) {
                    delivery.setRemoteState(disposition.getState());
                    delivery.setRemoteSettled(disposition.isSettled());
                    affectedDeliveries.add(delivery);

                    if (disposition.isSettled()) {
                        // Receiver settled, we can remove from unsettled
                        long tag = bytesToLong(delivery.getDeliveryTag());
                        unsettled.remove(tag);
                    }
                    break;
                }
            }
        }
        return affectedDeliveries;
    }

    /**
     * Get number of unsettled deliveries.
     */
    public int getUnsettledCount() {
        return unsettled.size();
    }

    /**
     * Check if there are unsettled deliveries.
     */
    public boolean hasUnsettled() {
        return !unsettled.isEmpty();
    }

    private byte[] longToBytes(long value) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return result;
    }

    private long bytesToLong(byte[] bytes) {
        long result = 0;
        for (int i = 0; i < Math.min(bytes.length, 8); i++) {
            result = (result << 8) | (bytes[i] & 0xFF);
        }
        return result;
    }

    /**
     * Represents a pending delivery from this sender.
     */
    public static class SenderDelivery {
        private final long deliveryId;
        private final byte[] deliveryTag;
        private final Message10 message;
        private final boolean localSettled;

        private Transfer transfer;
        private Object remoteState;
        private boolean remoteSettled;
        private byte[] txnId; // Transaction ID if transactional
        private String queueName; // Queue this message was delivered from (for requeue on Released)
        private Object internalMessage; // Internal broker message for requeue on Released

        public SenderDelivery(long deliveryId, byte[] deliveryTag, Message10 message, boolean settled) {
            this.deliveryId = deliveryId;
            this.deliveryTag = deliveryTag;
            this.message = message;
            this.localSettled = settled;
        }

        public long getDeliveryId() {
            return deliveryId;
        }

        public byte[] getDeliveryTag() {
            return deliveryTag;
        }

        public Message10 getMessage() {
            return message;
        }

        public boolean isLocalSettled() {
            return localSettled;
        }

        public Transfer getTransfer() {
            return transfer;
        }

        public void setTransfer(Transfer transfer) {
            this.transfer = transfer;
        }

        public Object getRemoteState() {
            return remoteState;
        }

        public void setRemoteState(Object remoteState) {
            this.remoteState = remoteState;
        }

        public boolean isRemoteSettled() {
            return remoteSettled;
        }

        public void setRemoteSettled(boolean remoteSettled) {
            this.remoteSettled = remoteSettled;
        }

        public boolean isFullySettled() {
            return localSettled && remoteSettled;
        }

        public byte[] getTxnId() {
            return txnId;
        }

        public void setTxnId(byte[] txnId) {
            this.txnId = txnId;
        }

        public boolean isTransactional() {
            return txnId != null;
        }

        public String getQueueName() {
            return queueName;
        }

        public void setQueueName(String queueName) {
            this.queueName = queueName;
        }

        public Object getInternalMessage() {
            return internalMessage;
        }

        public void setInternalMessage(Object internalMessage) {
            this.internalMessage = internalMessage;
        }
    }
}
