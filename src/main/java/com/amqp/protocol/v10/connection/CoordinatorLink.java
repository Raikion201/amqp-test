package com.amqp.protocol.v10.connection;

import com.amqp.protocol.v10.delivery.Accepted;
import com.amqp.protocol.v10.delivery.DeliveryState;
import com.amqp.protocol.v10.delivery.Rejected;
import com.amqp.protocol.v10.messaging.AmqpValue;
import com.amqp.protocol.v10.messaging.Message10;
import com.amqp.protocol.v10.server.Transaction10;
import com.amqp.protocol.v10.transaction.*;
import com.amqp.protocol.v10.transport.*;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AMQP 1.0 Coordinator Link.
 *
 * Handles transaction coordinator functionality. Clients attach to this link
 * to declare and discharge transactions.
 *
 * The coordinator link is a receiver link (from the broker's perspective)
 * because it receives Declare and Discharge messages from clients.
 */
public class CoordinatorLink extends Amqp10Link {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorLink.class);

    // Active transactions managed by this coordinator
    private final Map<String, Transaction10> transactions = new ConcurrentHashMap<>();

    // Partial transfer being assembled
    private CoordinatorDelivery currentDelivery;

    // Callback interface for transaction events
    private TransactionHandler transactionHandler;

    // Coordinator capabilities
    private Coordinator coordinator;

    public CoordinatorLink(Amqp10Session session, String name, long handle,
                           Source source, Coordinator coordinator) {
        super(session, name, handle, true, source, null); // role = true = receiver
        this.coordinator = coordinator;
    }

    /**
     * Handle an incoming transfer containing a Declare or Discharge message.
     */
    public DeliveryState onTransfer(Transfer transfer, ByteBuf payload) {
        if (transfer.getDeliveryId() != null && currentDelivery == null) {
            currentDelivery = new CoordinatorDelivery(
                    transfer.getDeliveryId(),
                    transfer.getDeliveryTag()
            );
        }

        // Add payload
        if (payload != null && payload.isReadable()) {
            currentDelivery.addPayload(payload);
        }

        // Check if complete
        if (!transfer.hasMore()) {
            try {
                DeliveryState result = processCoordinatorMessage(currentDelivery);
                currentDelivery.release();
                currentDelivery = null;
                return result;
            } catch (Exception e) {
                log.error("Error processing coordinator message", e);
                currentDelivery.release();
                currentDelivery = null;
                return createRejected("Internal error: " + e.getMessage());
            }
        }

        return null; // Still receiving more frames
    }

    /**
     * Process a complete coordinator message (Declare or Discharge).
     */
    private DeliveryState processCoordinatorMessage(CoordinatorDelivery delivery) {
        ByteBuf payload = delivery.getPayload();

        try {
            // Parse the message
            Message10 message = Message10.decode(payload);
            if (message == null || message.getBody() == null) {
                return createRejected("Empty coordinator message");
            }

            Object body = message.getBody();

            // Check if it's an AmqpValue containing a described type
            if (body instanceof AmqpValue) {
                body = ((AmqpValue) body).getValue();
            }

            if (body instanceof DescribedType) {
                DescribedType described = (DescribedType) body;
                Object descriptor = described.getDescriptor();
                long descriptorCode = descriptor instanceof Number
                        ? ((Number) descriptor).longValue()
                        : 0;

                if (descriptorCode == Declare.DESCRIPTOR) {
                    return handleDeclare(Declare.decode(described));
                } else if (descriptorCode == Discharge.DESCRIPTOR) {
                    return handleDischarge(Discharge.decode(described));
                }
            }

            // Unknown message type
            return createRejected("Unknown coordinator message type");

        } catch (Exception e) {
            log.error("Failed to parse coordinator message", e);
            return createRejected("Parse error: " + e.getMessage());
        }
    }

    /**
     * Handle a Declare message - create a new transaction.
     */
    private DeliveryState handleDeclare(Declare declare) {
        log.debug("Handling Declare: {}", declare);

        try {
            // Create new transaction
            Transaction10 txn = new Transaction10(session);
            String txnKey = txn.getTxnIdString();
            transactions.put(txnKey, txn);

            log.info("Transaction declared: {}", txnKey);

            // Notify handler if present
            if (transactionHandler != null) {
                transactionHandler.onTransactionDeclared(txn);
            }

            // Return Declared with the transaction ID
            return new Declared(txn.getTxnId());

        } catch (Exception e) {
            log.error("Failed to declare transaction", e);
            return createRejected("Failed to declare transaction: " + e.getMessage());
        }
    }

    /**
     * Handle a Discharge message - commit or rollback a transaction.
     */
    private DeliveryState handleDischarge(Discharge discharge) {
        log.debug("Handling Discharge: {}", discharge);

        byte[] txnId = discharge.getTxnId();
        if (txnId == null) {
            return createRejected("Missing transaction ID");
        }

        String txnKey = bytesToHex(txnId);
        Transaction10 txn = transactions.get(txnKey);

        if (txn == null) {
            // Also try to find by exact byte match
            txn = findTransactionByBytes(txnId);
        }

        if (txn == null) {
            return createRejected("Unknown transaction: " + txnKey);
        }

        try {
            if (discharge.shouldRollback()) {
                log.info("Rolling back transaction: {}", txnKey);
                txn.rollback();
                if (transactionHandler != null) {
                    transactionHandler.onTransactionRolledBack(txn);
                }
            } else {
                log.info("Committing transaction: {}", txnKey);
                txn.commit();
                if (transactionHandler != null) {
                    transactionHandler.onTransactionCommitted(txn);
                }
            }

            // Remove completed transaction
            transactions.remove(txnKey);

            return Accepted.INSTANCE;

        } catch (Exception e) {
            log.error("Failed to discharge transaction", e);
            transactions.remove(txnKey);
            return createRejected("Discharge failed: " + e.getMessage());
        }
    }

    /**
     * Find a transaction by byte array ID.
     */
    private Transaction10 findTransactionByBytes(byte[] txnId) {
        for (Transaction10 txn : transactions.values()) {
            if (Arrays.equals(txn.getTxnId(), txnId)) {
                return txn;
            }
        }
        return null;
    }

    /**
     * Get a transaction by ID.
     */
    public Transaction10 getTransaction(byte[] txnId) {
        String txnKey = bytesToHex(txnId);
        Transaction10 txn = transactions.get(txnKey);
        if (txn == null) {
            txn = findTransactionByBytes(txnId);
        }
        return txn;
    }

    /**
     * Get all active transactions.
     */
    public Map<String, Transaction10> getActiveTransactions() {
        return transactions;
    }

    /**
     * Create a Rejected delivery state with error.
     */
    private Rejected createRejected(String description) {
        Rejected rejected = new Rejected();
        rejected.setError(new ErrorCondition(
                Symbol.valueOf("amqp:transaction:error"),
                description
        ));
        return rejected;
    }

    /**
     * Create an attach for this coordinator link.
     */
    @Override
    public Attach createAttach() {
        Attach attach = super.createAttach();
        // Set coordinator as target
        if (coordinator != null) {
            attach.setCoordinator(coordinator);
        }
        return attach;
    }

    /**
     * Set the transaction handler.
     */
    public void setTransactionHandler(TransactionHandler handler) {
        this.transactionHandler = handler;
    }

    /**
     * Convert bytes to hex string.
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * Callback interface for transaction events.
     */
    public interface TransactionHandler {
        void onTransactionDeclared(Transaction10 transaction);
        void onTransactionCommitted(Transaction10 transaction);
        void onTransactionRolledBack(Transaction10 transaction);
    }

    /**
     * Represents a delivery to the coordinator.
     */
    private static class CoordinatorDelivery {
        private final long deliveryId;
        private final byte[] deliveryTag;
        private CompositeByteBuf payloadBuffer;

        public CoordinatorDelivery(long deliveryId, byte[] deliveryTag) {
            this.deliveryId = deliveryId;
            this.deliveryTag = deliveryTag;
            this.payloadBuffer = Unpooled.compositeBuffer();
        }

        public void addPayload(ByteBuf payload) {
            payloadBuffer.addComponent(true, payload.retainedSlice());
        }

        public ByteBuf getPayload() {
            return payloadBuffer;
        }

        public long getDeliveryId() {
            return deliveryId;
        }

        public byte[] getDeliveryTag() {
            return deliveryTag;
        }

        public void release() {
            if (payloadBuffer != null) {
                payloadBuffer.release();
                payloadBuffer = null;
            }
        }
    }

    @Override
    public String toString() {
        return String.format("CoordinatorLink{name='%s', handle=%d, transactions=%d}",
                name, handle, transactions.size());
    }
}
