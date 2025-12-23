package com.amqp.protocol.v10.server;

import com.amqp.protocol.v10.connection.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AMQP 1.0 Transaction Manager.
 *
 * Manages the lifecycle of a transaction including tracking all operations
 * (publishes and acks) associated with the transaction until commit or rollback.
 */
public class Transaction10 {

    private static final Logger log = LoggerFactory.getLogger(Transaction10.class);

    // Transaction ID counter for unique IDs
    private static final AtomicInteger txnCounter = new AtomicInteger(0);

    /**
     * Transaction states.
     */
    public enum State {
        DECLARED,   // Transaction has been declared, accepting operations
        DISCHARGING, // In process of being committed/rolled back
        COMMITTED,  // Successfully committed
        ROLLEDBACK  // Rolled back
    }

    private final byte[] txnId;
    private final String txnIdString;
    private volatile State state = State.DECLARED;

    // Tracked operations
    private final List<TransactionalDelivery> publishedMessages = new ArrayList<>();
    private final List<TransactionalAck> acknowledgments = new ArrayList<>();

    // Session association
    private final Object session;

    // Timestamp
    private final long createdAt;

    public Transaction10(Object session) {
        this.session = session;
        this.txnId = generateTxnId();
        this.txnIdString = bytesToHex(txnId);
        this.createdAt = System.currentTimeMillis();
        log.debug("Created transaction: {}", txnIdString);
    }

    /**
     * Generate a unique transaction ID.
     */
    private byte[] generateTxnId() {
        // Use combination of UUID and counter for uniqueness
        UUID uuid = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(20);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        buffer.putInt(txnCounter.incrementAndGet());
        return buffer.array();
    }

    /**
     * Convert bytes to hex string for logging.
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * Get the transaction ID.
     */
    public byte[] getTxnId() {
        return txnId;
    }

    /**
     * Get the transaction ID as a string.
     */
    public String getTxnIdString() {
        return txnIdString;
    }

    /**
     * Get current state.
     */
    public State getState() {
        return state;
    }

    /**
     * Check if transaction is still active.
     */
    public boolean isActive() {
        return state == State.DECLARED;
    }

    /**
     * Add a published message to this transaction.
     */
    public void addPublish(Delivery delivery, String address, byte[] messageData) {
        if (state != State.DECLARED) {
            throw new IllegalStateException("Transaction not active: " + state);
        }
        publishedMessages.add(new TransactionalDelivery(delivery, address, messageData));
        log.debug("Transaction {} added publish to {}", txnIdString, address);
    }

    /**
     * Add an acknowledgment to this transaction.
     */
    public void addAcknowledgment(Delivery delivery, long deliveryId) {
        if (state != State.DECLARED) {
            throw new IllegalStateException("Transaction not active: " + state);
        }
        acknowledgments.add(new TransactionalAck(delivery, deliveryId));
        log.debug("Transaction {} added ack for delivery {}", txnIdString, deliveryId);
    }

    /**
     * Commit the transaction.
     */
    public void commit() throws Exception {
        if (state != State.DECLARED) {
            throw new IllegalStateException("Transaction not in DECLARED state: " + state);
        }

        state = State.DISCHARGING;
        log.debug("Committing transaction {}: {} publishes, {} acks",
                txnIdString, publishedMessages.size(), acknowledgments.size());

        try {
            // First, make all published messages visible
            for (TransactionalDelivery delivery : publishedMessages) {
                delivery.commit();
            }

            // Then, finalize all acknowledgments
            for (TransactionalAck ack : acknowledgments) {
                ack.commit();
            }

            state = State.COMMITTED;
            log.debug("Transaction {} committed successfully", txnIdString);
        } catch (Exception e) {
            state = State.ROLLEDBACK;
            log.error("Transaction {} commit failed, rolling back", txnIdString, e);
            rollbackInternal();
            throw e;
        }
    }

    /**
     * Rollback the transaction.
     */
    public void rollback() {
        if (state != State.DECLARED) {
            if (state == State.ROLLEDBACK) {
                return; // Already rolled back
            }
            throw new IllegalStateException("Transaction not in DECLARED state: " + state);
        }

        state = State.DISCHARGING;
        log.debug("Rolling back transaction {}: {} publishes, {} acks",
                txnIdString, publishedMessages.size(), acknowledgments.size());

        rollbackInternal();
        state = State.ROLLEDBACK;
        log.debug("Transaction {} rolled back", txnIdString);
    }

    /**
     * Internal rollback implementation.
     */
    private void rollbackInternal() {
        // Rollback published messages (don't make them visible)
        for (TransactionalDelivery delivery : publishedMessages) {
            try {
                delivery.rollback();
            } catch (Exception e) {
                log.warn("Error rolling back publish in transaction {}", txnIdString, e);
            }
        }

        // Rollback acknowledgments (make messages available again)
        for (TransactionalAck ack : acknowledgments) {
            try {
                ack.rollback();
            } catch (Exception e) {
                log.warn("Error rolling back ack in transaction {}", txnIdString, e);
            }
        }
    }

    /**
     * Get the list of published deliveries.
     */
    public List<TransactionalDelivery> getPublishedMessages() {
        return publishedMessages;
    }

    /**
     * Get the list of acknowledgments.
     */
    public List<TransactionalAck> getAcknowledgments() {
        return acknowledgments;
    }

    /**
     * Get creation timestamp.
     */
    public long getCreatedAt() {
        return createdAt;
    }

    /**
     * Represents a message published within a transaction.
     */
    public static class TransactionalDelivery {
        private final Delivery delivery;
        private final String address;
        private final byte[] messageData;
        private boolean committed = false;

        public TransactionalDelivery(Delivery delivery, String address, byte[] messageData) {
            this.delivery = delivery;
            this.address = address;
            this.messageData = messageData;
        }

        public Delivery getDelivery() {
            return delivery;
        }

        public String getAddress() {
            return address;
        }

        public byte[] getMessageData() {
            return messageData;
        }

        public void commit() {
            // Mark as committed - the message becomes visible
            committed = true;
        }

        public void rollback() {
            // Discard the message
            committed = false;
        }

        public boolean isCommitted() {
            return committed;
        }
    }

    /**
     * Represents an acknowledgment within a transaction.
     */
    public static class TransactionalAck {
        private final Delivery delivery;
        private final long deliveryId;
        private boolean committed = false;

        public TransactionalAck(Delivery delivery, long deliveryId) {
            this.delivery = delivery;
            this.deliveryId = deliveryId;
        }

        public Delivery getDelivery() {
            return delivery;
        }

        public long getDeliveryId() {
            return deliveryId;
        }

        public void commit() {
            // Finalize the ack - message is removed from queue
            committed = true;
        }

        public void rollback() {
            // Make the message available again (redeliver)
            committed = false;
        }

        public boolean isCommitted() {
            return committed;
        }
    }

    @Override
    public String toString() {
        return String.format("Transaction10{id=%s, state=%s, publishes=%d, acks=%d}",
                txnIdString, state, publishedMessages.size(), acknowledgments.size());
    }
}
