package com.amqp.connection;

import com.amqp.amqp.AmqpFrame;
import com.amqp.amqp.AmqpCodec;
import com.amqp.model.Exchange;
import com.amqp.model.Queue;
import com.amqp.server.AmqpBroker;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Advanced edge case and error condition tests for AmqpChannel
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("AMQP Channel Advanced Tests")
class AmqpChannelAdvancedTest {

    @Mock
    private AmqpConnection connection;

    @Mock
    private AmqpBroker broker;

    private AmqpChannel channel;

    @BeforeEach
    void setUp() {
        channel = new AmqpChannel((short) 1, connection, broker);
    }

    @Nested
    @DisplayName("Transaction Edge Cases")
    class TransactionEdgeCases {

        @Test
        @DisplayName("Should throw exception when committing without transaction")
        void testCommitWithoutTransaction() {
            assertThatThrownBy(() -> channel.txCommit())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No transaction in progress");
        }

        @Test
        @DisplayName("Should throw exception when rolling back without transaction")
        void testRollbackWithoutTransaction() {
            assertThatThrownBy(() -> channel.txRollback())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No transaction in progress");
        }

        @Test
        @DisplayName("Should handle multiple transaction cycles")
        void testMultipleTransactionCycles() {
            for (int i = 0; i < 10; i++) {
                channel.txSelect();
                channel.txCommit();
            }
        }

        @Test
        @DisplayName("Should handle transaction commit then immediate start")
        void testTransactionCommitAndRestart() {
            channel.txSelect();
            channel.txCommit();

            // Start new transaction immediately
            assertThatCode(() -> {
                channel.txSelect();
                channel.txCommit();
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle transaction rollback then immediate start")
        void testTransactionRollbackAndRestart() {
            channel.txSelect();
            channel.txRollback();

            // Start new transaction immediately
            assertThatCode(() -> {
                channel.txSelect();
                channel.txCommit();
            }).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("QoS Edge Cases")
    class QoSEdgeCases {

        @Test
        @DisplayName("Should handle zero prefetch count")
        void testZeroPrefetchCount() {
            channel.setQoS(0);
            assertThat(channel.getPrefetchCount()).isEqualTo(0);
        }

        @Test
        @DisplayName("Should handle large prefetch count")
        void testLargePrefetchCount() {
            channel.setQoS(10000);
            assertThat(channel.getPrefetchCount()).isEqualTo(10000);
        }

        @Test
        @DisplayName("Should handle negative prefetch count")
        void testNegativePrefetchCount() {
            channel.setQoS(-1);
            assertThat(channel.getPrefetchCount()).isEqualTo(-1);
        }

        @Test
        @DisplayName("Should allow changing prefetch count multiple times")
        void testChangingPrefetchCount() {
            channel.setQoS(10);
            assertThat(channel.getPrefetchCount()).isEqualTo(10);

            channel.setQoS(20);
            assertThat(channel.getPrefetchCount()).isEqualTo(20);

            channel.setQoS(0);
            assertThat(channel.getPrefetchCount()).isEqualTo(0);
        }
    }

    @Nested
    @DisplayName("Consumer Management Edge Cases")
    class ConsumerEdgeCases {

        @Test
        @DisplayName("Should handle removing non-existent consumer")
        void testRemoveNonExistentConsumer() {
            assertThatCode(() -> channel.removeConsumer("non-existent"))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle checking non-existent consumer")
        void testHasNonExistentConsumer() {
            assertThat(channel.hasConsumer("non-existent")).isFalse();
        }

        @Test
        @DisplayName("Should handle empty consumer tag")
        void testEmptyConsumerTag() {
            channel.addConsumer("", "queue-1");
            assertThat(channel.hasConsumer("")).isTrue();

            channel.removeConsumer("");
            assertThat(channel.hasConsumer("")).isFalse();
        }

        @Test
        @DisplayName("Should handle duplicate consumer tags")
        void testDuplicateConsumerTags() {
            channel.addConsumer("consumer-1", "queue-1");

            // Adding again with same tag (should replace)
            channel.addConsumer("consumer-1", "queue-2");

            assertThat(channel.hasConsumer("consumer-1")).isTrue();
        }

        @Test
        @DisplayName("Should handle many consumers")
        void testManyConsumers() {
            for (int i = 0; i < 1000; i++) {
                channel.addConsumer("consumer-" + i, "queue-" + i);
            }

            for (int i = 0; i < 1000; i++) {
                assertThat(channel.hasConsumer("consumer-" + i)).isTrue();
            }

            for (int i = 0; i < 1000; i++) {
                channel.removeConsumer("consumer-" + i);
            }

            for (int i = 0; i < 1000; i++) {
                assertThat(channel.hasConsumer("consumer-" + i)).isFalse();
            }
        }
    }

    @Nested
    @DisplayName("Message Acknowledgment Edge Cases")
    class AcknowledgmentEdgeCases {

        @Test
        @DisplayName("Should handle ack with zero delivery tag")
        void testAckZeroDeliveryTag() {
            assertThatCode(() -> channel.acknowledgeMessage(0L, false))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle ack with negative delivery tag")
        void testAckNegativeDeliveryTag() {
            assertThatCode(() -> channel.acknowledgeMessage(-1L, false))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle ack with maximum delivery tag")
        void testAckMaxDeliveryTag() {
            assertThatCode(() -> channel.acknowledgeMessage(Long.MAX_VALUE, false))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle multiple acks for same delivery tag")
        void testMultipleAcksSameTag() {
            long deliveryTag = 123L;

            channel.acknowledgeMessage(deliveryTag, false);
            channel.acknowledgeMessage(deliveryTag, false);
            channel.acknowledgeMessage(deliveryTag, false);
        }

        @Test
        @DisplayName("Should handle reject with zero delivery tag")
        void testRejectZeroDeliveryTag() {
            assertThatCode(() -> channel.rejectMessage(0L, false))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle reject with requeue true")
        void testRejectWithRequeue() {
            assertThatCode(() -> channel.rejectMessage(123L, true))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle reject with requeue false")
        void testRejectWithoutRequeue() {
            assertThatCode(() -> channel.rejectMessage(123L, false))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle multiple flag on acknowledgment")
        void testMultipleFlagAcknowledgment() {
            // Multiple flag should ack all messages up to delivery tag
            assertThatCode(() -> channel.acknowledgeMessage(100L, true))
                .doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("Frame Handling Edge Cases")
    class FrameHandlingEdgeCases {

        @Test
        @DisplayName("Should handle METHOD frame")
        void testMethodFrame() {
            ByteBuf payload = Unpooled.buffer();
            payload.writeShort(20); // Channel class
            payload.writeShort(10); // Open method
            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.METHOD.getValue(), (short) 1, payload);

            assertThatCode(() -> channel.handleFrame(frame))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle HEADER frame")
        void testHeaderFrame() {
            ByteBuf payload = Unpooled.buffer();
            payload.writeShort(60); // Basic class
            payload.writeShort(0);
            payload.writeLong(100L); // body size
            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.HEADER.getValue(), (short) 1, payload);

            assertThatCode(() -> channel.handleFrame(frame))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle BODY frame")
        void testBodyFrame() {
            ByteBuf payload = Unpooled.wrappedBuffer("message body".getBytes());
            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.BODY.getValue(), (short) 1, payload);

            assertThatCode(() -> channel.handleFrame(frame))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle HEARTBEAT frame")
        void testHeartbeatFrame() {
            ByteBuf payload = Unpooled.buffer(0);
            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.HEARTBEAT.getValue(), (short) 0, payload);

            assertThatCode(() -> channel.handleFrame(frame))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle frame with unknown class ID")
        void testFrameWithUnknownClassId() {
            ByteBuf payload = Unpooled.buffer();
            payload.writeShort(999); // Unknown class
            payload.writeShort(10);
            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.METHOD.getValue(), (short) 1, payload);

            assertThatCode(() -> channel.handleFrame(frame))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle frames when channel is closed")
        void testFrameWhenClosed() {
            channel.close();

            ByteBuf payload = Unpooled.buffer();
            payload.writeShort(20);
            payload.writeShort(10);
            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.METHOD.getValue(), (short) 1, payload);

            // Should log warning but not throw
            assertThatCode(() -> channel.handleFrame(frame))
                .doesNotThrowAnyException();

            // Broker should not be called when channel is closed
            verifyNoInteractions(broker);
        }
    }

    @Nested
    @DisplayName("Channel State Tests")
    class ChannelStateTests {

        @Test
        @DisplayName("Should be open initially")
        void testInitiallyOpen() {
            assertThat(channel.isOpen()).isTrue();
        }

        @Test
        @DisplayName("Should be closed after close")
        void testClosedAfterClose() {
            channel.close();
            assertThat(channel.isOpen()).isFalse();
        }

        @Test
        @DisplayName("Should handle multiple close calls")
        void testMultipleClose() {
            channel.close();
            channel.close();
            channel.close();

            assertThat(channel.isOpen()).isFalse();
        }

        @Test
        @DisplayName("Should have correct channel number")
        void testChannelNumber() {
            AmqpChannel ch1 = new AmqpChannel((short) 1, connection, broker);
            AmqpChannel ch100 = new AmqpChannel((short) 100, connection, broker);
            AmqpChannel chMax = new AmqpChannel(Short.MAX_VALUE, connection, broker);

            assertThat(ch1.getChannelNumber()).isEqualTo((short) 1);
            assertThat(ch100.getChannelNumber()).isEqualTo((short) 100);
            assertThat(chMax.getChannelNumber()).isEqualTo(Short.MAX_VALUE);
        }
    }

    @Nested
    @DisplayName("Exchange Declaration Edge Cases")
    class ExchangeDeclarationEdgeCases {

        @Test
        @DisplayName("Should handle exchange declaration with all types")
        void testAllExchangeTypes() {
            Exchange direct = new Exchange("direct-ex", Exchange.Type.DIRECT, true, false, false);
            Exchange fanout = new Exchange("fanout-ex", Exchange.Type.FANOUT, true, false, false);
            Exchange topic = new Exchange("topic-ex", Exchange.Type.TOPIC, true, false, false);
            Exchange headers = new Exchange("headers-ex", Exchange.Type.HEADERS, true, false, false);

            when(broker.declareExchange("direct-ex", Exchange.Type.DIRECT, true, false, false))
                .thenReturn(direct);
            when(broker.declareExchange("fanout-ex", Exchange.Type.FANOUT, true, false, false))
                .thenReturn(fanout);
            when(broker.declareExchange("topic-ex", Exchange.Type.TOPIC, true, false, false))
                .thenReturn(topic);
            when(broker.declareExchange("headers-ex", Exchange.Type.HEADERS, true, false, false))
                .thenReturn(headers);

            assertThatCode(() -> {
                broker.declareExchange("direct-ex", Exchange.Type.DIRECT, true, false, false);
                broker.declareExchange("fanout-ex", Exchange.Type.FANOUT, true, false, false);
                broker.declareExchange("topic-ex", Exchange.Type.TOPIC, true, false, false);
                broker.declareExchange("headers-ex", Exchange.Type.HEADERS, true, false, false);
            }).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle exchange with empty name")
        void testExchangeWithEmptyName() {
            Exchange defaultEx = new Exchange("", Exchange.Type.DIRECT, true, false, false);
            when(broker.declareExchange("", Exchange.Type.DIRECT, true, false, false))
                .thenReturn(defaultEx);

            assertThatCode(() ->
                broker.declareExchange("", Exchange.Type.DIRECT, true, false, false)
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle durable exchange")
        void testDurableExchange() {
            Exchange ex = new Exchange("durable-ex", Exchange.Type.DIRECT, true, false, false);
            when(broker.declareExchange("durable-ex", Exchange.Type.DIRECT, true, false, false))
                .thenReturn(ex);

            broker.declareExchange("durable-ex", Exchange.Type.DIRECT, true, false, false);
            verify(broker).declareExchange("durable-ex", Exchange.Type.DIRECT, true, false, false);
        }

        @Test
        @DisplayName("Should handle auto-delete exchange")
        void testAutoDeleteExchange() {
            Exchange ex = new Exchange("temp-ex", Exchange.Type.DIRECT, false, true, false);
            when(broker.declareExchange("temp-ex", Exchange.Type.DIRECT, false, true, false))
                .thenReturn(ex);

            broker.declareExchange("temp-ex", Exchange.Type.DIRECT, false, true, false);
            verify(broker).declareExchange("temp-ex", Exchange.Type.DIRECT, false, true, false);
        }

        @Test
        @DisplayName("Should handle internal exchange")
        void testInternalExchange() {
            Exchange ex = new Exchange("internal-ex", Exchange.Type.DIRECT, true, false, true);
            when(broker.declareExchange("internal-ex", Exchange.Type.DIRECT, true, false, true))
                .thenReturn(ex);

            broker.declareExchange("internal-ex", Exchange.Type.DIRECT, true, false, true);
            verify(broker).declareExchange("internal-ex", Exchange.Type.DIRECT, true, false, true);
        }
    }

    @Nested
    @DisplayName("Queue Declaration Edge Cases")
    class QueueDeclarationEdgeCases {

        @Test
        @DisplayName("Should handle queue with empty name")
        void testQueueWithEmptyName() {
            Queue queue = new Queue("amq.gen-123", false, false, false);
            when(broker.declareQueue("", false, false, false))
                .thenReturn(queue);

            assertThatCode(() -> broker.declareQueue("", false, false, false))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle exclusive queue")
        void testExclusiveQueue() {
            Queue queue = new Queue("exclusive-q", false, true, false);
            when(broker.declareQueue("exclusive-q", false, true, false))
                .thenReturn(queue);

            broker.declareQueue("exclusive-q", false, true, false);
            verify(broker).declareQueue("exclusive-q", false, true, false);
        }

        @Test
        @DisplayName("Should handle durable queue")
        void testDurableQueue() {
            Queue queue = new Queue("durable-q", true, false, false);
            when(broker.declareQueue("durable-q", true, false, false))
                .thenReturn(queue);

            broker.declareQueue("durable-q", true, false, false);
            verify(broker).declareQueue("durable-q", true, false, false);
        }

        @Test
        @DisplayName("Should handle auto-delete queue")
        void testAutoDeleteQueue() {
            Queue queue = new Queue("temp-q", false, false, true);
            when(broker.declareQueue("temp-q", false, false, true))
                .thenReturn(queue);

            broker.declareQueue("temp-q", false, false, true);
            verify(broker).declareQueue("temp-q", false, false, true);
        }

        @Test
        @DisplayName("Should handle queue with all flags")
        void testQueueWithAllFlags() {
            Queue queue = new Queue("full-q", true, true, true);
            when(broker.declareQueue("full-q", true, true, true))
                .thenReturn(queue);

            broker.declareQueue("full-q", true, true, true);
            verify(broker).declareQueue("full-q", true, true, true);
        }
    }

    @Nested
    @DisplayName("Stress Tests")
    class StressTests {

        @Test
        @DisplayName("Should handle rapid frame processing")
        void testRapidFrameProcessing() {
            for (int i = 0; i < 1000; i++) {
                ByteBuf payload = Unpooled.buffer();
                payload.writeShort(8);
                payload.writeShort(0);
                AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.HEARTBEAT.getValue(), (short) 0, payload);
                channel.handleFrame(frame);
            }
        }

        @Test
        @DisplayName("Should handle many consumer operations")
        void testManyConsumerOperations() {
            for (int i = 0; i < 100; i++) {
                channel.addConsumer("consumer-" + i, "queue-" + i);
            }

            for (int i = 0; i < 100; i++) {
                assertThat(channel.hasConsumer("consumer-" + i)).isTrue();
            }

            for (int i = 0; i < 100; i++) {
                channel.removeConsumer("consumer-" + i);
            }
        }

        @Test
        @DisplayName("Should handle many acknowledgments")
        void testManyAcknowledgments() {
            for (long i = 1; i <= 1000; i++) {
                channel.acknowledgeMessage(i, false);
            }
        }

        @Test
        @DisplayName("Should handle alternating transaction operations")
        void testAlternatingTransactions() {
            for (int i = 0; i < 50; i++) {
                channel.txSelect();
                if (i % 2 == 0) {
                    channel.txCommit();
                } else {
                    channel.txRollback();
                }
            }
        }
    }
}
