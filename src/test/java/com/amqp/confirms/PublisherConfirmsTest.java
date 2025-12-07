package com.amqp.confirms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.*;

class PublisherConfirmsTest {

    private PublisherConfirms confirms;

    @BeforeEach
    void setUp() {
        confirms = new PublisherConfirms();
    }

    @Test
    void testEnableConfirmMode() {
        assertThat(confirms.isConfirmMode()).isFalse();

        confirms.enableConfirmMode();

        assertThat(confirms.isConfirmMode()).isTrue();
    }

    @Test
    void testPublishSequenceNumbers() {
        confirms.enableConfirmMode();

        long seq1 = confirms.nextPublishSeqNo();
        long seq2 = confirms.nextPublishSeqNo();
        long seq3 = confirms.nextPublishSeqNo();

        assertThat(seq1).isEqualTo(1);
        assertThat(seq2).isEqualTo(2);
        assertThat(seq3).isEqualTo(3);
    }

    @Test
    void testAddPendingConfirm() {
        confirms.enableConfirmMode();

        long deliveryTag = confirms.nextPublishSeqNo();
        confirms.addPendingConfirm(deliveryTag, "test-exchange", "test-key", "body".getBytes());

        assertThat(confirms.getPendingConfirmCount()).isEqualTo(1);
    }

    @Test
    void testConfirmSingleMessage() {
        confirms.enableConfirmMode();

        AtomicLong confirmedTag = new AtomicLong(0);
        confirms.setConfirmListener(new PublisherConfirms.ConfirmListener() {
            @Override
            public void onConfirm(long deliveryTag, boolean multiple) {
                confirmedTag.set(deliveryTag);
            }

            @Override
            public void onReturn(long deliveryTag, int replyCode, String replyText,
                               String exchange, String routingKey, byte[] body) {
            }
        });

        long tag = confirms.nextPublishSeqNo();
        confirms.addPendingConfirm(tag, "exchange", "key", "body".getBytes());
        confirms.confirmMessage(tag, false, true);

        assertThat(confirmedTag.get()).isEqualTo(tag);
        assertThat(confirms.getPendingConfirmCount()).isEqualTo(0);
    }

    @Test
    void testConfirmMultipleMessages() {
        confirms.enableConfirmMode();

        AtomicInteger confirmCount = new AtomicInteger(0);
        confirms.setConfirmListener(new PublisherConfirms.ConfirmListener() {
            @Override
            public void onConfirm(long deliveryTag, boolean multiple) {
                confirmCount.incrementAndGet();
            }

            @Override
            public void onReturn(long deliveryTag, int replyCode, String replyText,
                               String exchange, String routingKey, byte[] body) {
            }
        });

        // Add 3 pending confirms
        for (int i = 0; i < 3; i++) {
            long tag = confirms.nextPublishSeqNo();
            confirms.addPendingConfirm(tag, "exchange", "key", "body".getBytes());
        }

        // Confirm all up to tag 3
        confirms.confirmMessage(3, true, true);

        assertThat(confirmCount.get()).isEqualTo(3);
        assertThat(confirms.getPendingConfirmCount()).isEqualTo(0);
    }

    @Test
    void testRejectMessage() {
        confirms.enableConfirmMode();

        AtomicInteger returnCount = new AtomicInteger(0);
        confirms.setConfirmListener(new PublisherConfirms.ConfirmListener() {
            @Override
            public void onConfirm(long deliveryTag, boolean multiple) {
            }

            @Override
            public void onReturn(long deliveryTag, int replyCode, String replyText,
                               String exchange, String routingKey, byte[] body) {
                returnCount.incrementAndGet();
            }
        });

        long tag = confirms.nextPublishSeqNo();
        confirms.addPendingConfirm(tag, "exchange", "key", "body".getBytes());
        confirms.rejectMessage(tag, 404, "NOT_FOUND");

        assertThat(returnCount.get()).isEqualTo(1);
        assertThat(confirms.getPendingConfirmCount()).isEqualTo(0);
    }

    @Test
    void testReturnUnroutableMessage() {
        confirms.enableConfirmMode();

        AtomicInteger returnCount = new AtomicInteger(0);
        AtomicInteger confirmCount = new AtomicInteger(0);

        confirms.setConfirmListener(new PublisherConfirms.ConfirmListener() {
            @Override
            public void onConfirm(long deliveryTag, boolean multiple) {
                confirmCount.incrementAndGet();
            }

            @Override
            public void onReturn(long deliveryTag, int replyCode, String replyText,
                               String exchange, String routingKey, byte[] body) {
                returnCount.incrementAndGet();
            }
        });

        long tag = confirms.nextPublishSeqNo();
        confirms.addPendingConfirm(tag, "exchange", "key", "body".getBytes());
        confirms.returnUnroutableMessage(tag, "exchange", "key", "body".getBytes());

        // Should get both return and confirm
        assertThat(returnCount.get()).isEqualTo(1);
        assertThat(confirmCount.get()).isEqualTo(1);
    }

    @Test
    void testClear() {
        confirms.enableConfirmMode();

        long tag = confirms.nextPublishSeqNo();
        confirms.addPendingConfirm(tag, "exchange", "key", "body".getBytes());

        confirms.clear();

        assertThat(confirms.isConfirmMode()).isFalse();
        assertThat(confirms.getPendingConfirmCount()).isEqualTo(0);
        assertThat(confirms.getNextPublishSeqNo()).isEqualTo(1);
    }
}
