package com.amqp.confirms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class PublisherConfirms {
    private static final Logger logger = LoggerFactory.getLogger(PublisherConfirms.class);

    private volatile boolean confirmMode = false;
    private final AtomicLong deliveryTag = new AtomicLong(0);
    private final ConcurrentMap<Long, PendingConfirm> pendingConfirms = new ConcurrentHashMap<>();
    private ConfirmListener confirmListener;

    public interface ConfirmListener {
        void onConfirm(long deliveryTag, boolean multiple);
        void onReturn(long deliveryTag, int replyCode, String replyText,
                     String exchange, String routingKey, byte[] body);
    }

    public void enableConfirmMode() {
        this.confirmMode = true;
        logger.debug("Publisher confirms enabled");
    }

    public boolean isConfirmMode() {
        return confirmMode;
    }

    public void setConfirmListener(ConfirmListener listener) {
        this.confirmListener = listener;
    }

    public long nextPublishSeqNo() {
        return deliveryTag.incrementAndGet();
    }

    public long getNextPublishSeqNo() {
        return deliveryTag.get() + 1;
    }

    public void addPendingConfirm(long deliveryTag, String exchange, String routingKey, byte[] body) {
        if (!confirmMode) {
            return;
        }

        PendingConfirm confirm = new PendingConfirm(deliveryTag, exchange, routingKey, body);
        pendingConfirms.put(deliveryTag, confirm);

        logger.debug("Added pending confirm for delivery tag: {}", deliveryTag);
    }

    public void confirmMessage(long deliveryTag, boolean multiple, boolean success) {
        if (!confirmMode) {
            return;
        }

        if (multiple) {
            // Confirm all messages up to and including deliveryTag
            for (long tag = 1; tag <= deliveryTag; tag++) {
                PendingConfirm confirm = pendingConfirms.remove(tag);
                if (confirm != null) {
                    if (success && confirmListener != null) {
                        confirmListener.onConfirm(tag, false);
                    }
                }
            }

            logger.debug("Confirmed multiple messages up to delivery tag: {}", deliveryTag);
        } else {
            // Confirm single message
            PendingConfirm confirm = pendingConfirms.remove(deliveryTag);
            if (confirm != null && success && confirmListener != null) {
                confirmListener.onConfirm(deliveryTag, false);
            }

            logger.debug("Confirmed single message with delivery tag: {}", deliveryTag);
        }
    }

    public void rejectMessage(long deliveryTag, int replyCode, String replyText) {
        if (!confirmMode) {
            return;
        }

        PendingConfirm confirm = pendingConfirms.remove(deliveryTag);
        if (confirm != null && confirmListener != null) {
            confirmListener.onReturn(deliveryTag, replyCode, replyText,
                                   confirm.exchange, confirm.routingKey, confirm.body);
        }

        logger.debug("Rejected message with delivery tag: {} - {}", deliveryTag, replyText);
    }

    public void returnUnroutableMessage(long deliveryTag, String exchange,
                                       String routingKey, byte[] body) {
        if (!confirmMode) {
            return;
        }

        if (confirmListener != null) {
            confirmListener.onReturn(deliveryTag, 312, "NO_ROUTE",
                                   exchange, routingKey, body);
        }

        // Still confirm the message was processed
        confirmMessage(deliveryTag, false, true);

        logger.debug("Returned unroutable message with delivery tag: {}", deliveryTag);
    }

    public int getPendingConfirmCount() {
        return pendingConfirms.size();
    }

    public void clear() {
        pendingConfirms.clear();
        deliveryTag.set(0);
        confirmMode = false;
        logger.debug("Publisher confirms cleared");
    }

    private static class PendingConfirm {
        final long deliveryTag;
        final String exchange;
        final String routingKey;
        final byte[] body;
        final long timestamp;

        PendingConfirm(long deliveryTag, String exchange, String routingKey, byte[] body) {
            this.deliveryTag = deliveryTag;
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.body = body;
            this.timestamp = System.currentTimeMillis();
        }
    }
}
