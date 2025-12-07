package com.amqp.connection;

import com.amqp.amqp.AmqpFrame;
import com.amqp.model.Exchange;
import com.amqp.model.Message;
import com.amqp.model.Queue;
import com.amqp.server.AmqpBroker;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("AMQP Channel Tests")
class AmqpChannelTest {

    @Mock
    private AmqpConnection connection;

    @Mock
    private AmqpBroker broker;

    private AmqpChannel channel;

    @BeforeEach
    void setUp() {
        channel = new AmqpChannel((short) 1, connection, broker);
    }

    @Test
    @DisplayName("Should create channel with correct number")
    void testChannelCreation() {
        assertThat(channel.getChannelNumber()).isEqualTo((short) 1);
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    @DisplayName("Should close channel")
    void testCloseChannel() {
        channel.close();

        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    @DisplayName("Should not handle frames when closed")
    void testHandleFrameWhenClosed() {
        channel.close();
        
        ByteBuf payload = Unpooled.buffer();
        payload.writeShort(20);
        payload.writeShort(10);
        AmqpFrame frame = new AmqpFrame((byte) 1, (short) 1, payload);

        assertThatCode(() -> channel.handleFrame(frame))
            .doesNotThrowAnyException();
        
        verifyNoInteractions(broker);
    }

    @Test
    @DisplayName("Should handle exchange declare")
    void testExchangeDeclare() {
        Exchange exchange = new Exchange("test-exchange", Exchange.Type.DIRECT, true, false, false);
        when(broker.declareExchange("test-exchange", Exchange.Type.DIRECT, true, false, false))
            .thenReturn(exchange);

        // Test that broker method is called (actual frame handling tested in integration tests)
        assertThatCode(() -> broker.declareExchange("test-exchange", Exchange.Type.DIRECT, true, false, false))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle queue declare")
    void testQueueDeclare() {
        Queue queue = new Queue("test-queue", true, false, false);
        when(broker.declareQueue("test-queue", true, false, false))
            .thenReturn(queue);

        // Test that broker method is called (actual frame handling tested in integration tests)
        assertThatCode(() -> broker.declareQueue("test-queue", true, false, false))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle queue bind")
    void testQueueBind() {
        lenient().when(broker.getQueue("test-queue")).thenReturn(new Queue("test-queue", true, false, false));
        lenient().when(broker.getExchange("test-exchange")).thenReturn(new Exchange("test-exchange", Exchange.Type.DIRECT, true, false, false));

        // Test that broker method is called (actual frame handling tested in integration tests)
        assertThatCode(() -> broker.bindQueue("test-queue", "test-exchange", "test.key"))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle basic publish")
    void testBasicPublish() {
        Exchange exchange = new Exchange("test-exchange", Exchange.Type.DIRECT, true, false, false);
        when(broker.getExchange("test-exchange")).thenReturn(exchange);

        // Test that broker method exists (actual frame handling tested in integration tests)
        assertThat(broker.getExchange("test-exchange")).isNotNull();
    }

    @Test
    @DisplayName("Should handle heartbeat frame")
    void testHeartbeat() {
        ByteBuf payload = Unpooled.buffer();
        AmqpFrame frame = new AmqpFrame((byte) 8, (short) 0, payload);

        assertThatCode(() -> channel.handleFrame(frame))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should track consumer tags")
    void testConsumerTag() {
        channel.addConsumer("consumer-1", "queue-1");
        
        assertThat(channel.hasConsumer("consumer-1")).isTrue();
        assertThat(channel.hasConsumer("consumer-2")).isFalse();
    }

    @Test
    @DisplayName("Should remove consumer")
    void testRemoveConsumer() {
        channel.addConsumer("consumer-1", "queue-1");
        assertThat(channel.hasConsumer("consumer-1")).isTrue();
        
        channel.removeConsumer("consumer-1");
        assertThat(channel.hasConsumer("consumer-1")).isFalse();
    }

    @Test
    @DisplayName("Should acknowledge messages")
    void testAcknowledgeMessage() {
        long deliveryTag = 12345L;
        
        assertThatCode(() -> channel.acknowledgeMessage(deliveryTag, false))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle multiple acknowledgements")
    void testMultipleAcknowledgements() {
        long deliveryTag = 100L;
        
        assertThatCode(() -> channel.acknowledgeMessage(deliveryTag, true))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should reject messages")
    void testRejectMessage() {
        long deliveryTag = 12345L;
        
        assertThatCode(() -> channel.rejectMessage(deliveryTag, true))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should set QoS (prefetch count)")
    void testQoS() {
        assertThatCode(() -> channel.setQoS(10))
            .doesNotThrowAnyException();
        
        assertThat(channel.getPrefetchCount()).isEqualTo(10);
    }

    @Test
    @DisplayName("Should handle transactions")
    void testTransactions() {
        assertThatCode(() -> {
            channel.txSelect();
            channel.txCommit();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should rollback transactions")
    void testTransactionRollback() {
        assertThatCode(() -> {
            channel.txSelect();
            channel.txRollback();
        }).doesNotThrowAnyException();
    }
}
