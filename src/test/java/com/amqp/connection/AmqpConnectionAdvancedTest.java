package com.amqp.connection;

import com.amqp.amqp.AmqpFrame;
import com.amqp.server.AmqpBroker;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
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
 * Advanced edge case and error condition tests for AmqpConnection
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("AMQP Connection Advanced Tests")
class AmqpConnectionAdvancedTest {

    @Mock
    private Channel nettyChannel;

    @Mock
    private AmqpBroker broker;

    private AmqpConnection connection;

    @BeforeEach
    void setUp() {
        lenient().when(nettyChannel.isActive()).thenReturn(true);
        connection = new AmqpConnection(nettyChannel, broker);
    }

    @Nested
    @DisplayName("Channel Limit Tests")
    class ChannelLimitTests {

        @Test
        @DisplayName("Should handle maximum channel number")
        void testMaximumChannelNumber() {
            assertThatCode(() ->
                connection.openChannel(Short.MAX_VALUE)
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle channel number zero (reserved)")
        void testChannelZero() {
            // Channel 0 is created by default in constructor
            assertThatThrownBy(() ->
                connection.openChannel((short) 0)
            ).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Should open many channels concurrently")
        void testManyConcurrentChannels() {
            for (short i = 1; i <= 100; i++) {
                connection.openChannel(i);
            }

            // Verify all channels can be closed
            for (short i = 1; i <= 100; i++) {
                connection.closeChannel(i);
            }
        }
    }

    @Nested
    @DisplayName("Frame Handling Edge Cases")
    class FrameHandlingEdgeCases {

        @Test
        @DisplayName("Should handle frame for non-existent channel")
        void testFrameForNonExistentChannel() {
            ByteBuf payload = Unpooled.buffer();
            payload.writeShort(20);
            payload.writeShort(10);
            AmqpFrame frame = new AmqpFrame((byte) 1, (short) 99, payload);

            // Should not throw exception, just log warning
            assertThatCode(() -> connection.handleFrame(frame))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle frame with empty payload")
        void testFrameWithEmptyPayload() {
            connection.openChannel((short) 1);

            ByteBuf payload = Unpooled.buffer(0);
            AmqpFrame frame = new AmqpFrame((byte) 8, (short) 1, payload);

            assertThatCode(() -> connection.handleFrame(frame))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle frame with large payload")
        void testFrameWithLargePayload() {
            connection.openChannel((short) 1);

            byte[] largeData = new byte[1024 * 1024]; // 1MB
            ByteBuf payload = Unpooled.wrappedBuffer(largeData);
            AmqpFrame frame = new AmqpFrame((byte) 3, (short) 1, payload);

            assertThatCode(() -> connection.handleFrame(frame))
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle malformed frame payload gracefully")
        void testMalformedFramePayload() {
            connection.openChannel((short) 1);

            ByteBuf payload = Unpooled.buffer();
            payload.writeByte(0xFF); // Invalid data
            AmqpFrame frame = new AmqpFrame((byte) 1, (short) 1, payload);

            // Should handle gracefully and close channel
            assertThatCode(() -> connection.handleFrame(frame))
                .doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("Send Method Edge Cases")
    class SendMethodEdgeCases {

        @Test
        @DisplayName("Should handle sending on inactive channel")
        void testSendOnInactiveChannel() {
            when(nettyChannel.isActive()).thenReturn(false);

            ByteBuf payload = Unpooled.buffer();
            payload.writeInt(123);

            connection.sendMethod((short) 1, (short) 10, (short) 20, payload);

            // Should not attempt to send
            verify(nettyChannel, never()).writeAndFlush(any());
        }

        @Test
        @DisplayName("Should handle sending with null payload")
        void testSendMethodWithNullPayload() {
            connection.sendMethod((short) 1, (short) 10, (short) 20, null);

            verify(nettyChannel, times(1)).writeAndFlush(any(AmqpFrame.class));
        }

        @Test
        @DisplayName("Should handle sending content header")
        void testSendContentHeader() {
            ByteBuf properties = Unpooled.buffer();
            properties.writeInt(12345);

            assertThatCode(() ->
                connection.sendContentHeader((short) 1, (short) 60, 1000L, properties)
            ).doesNotThrowAnyException();

            verify(nettyChannel).writeAndFlush(any(AmqpFrame.class));
        }

        @Test
        @DisplayName("Should handle sending content header with null properties")
        void testSendContentHeaderNullProperties() {
            assertThatCode(() ->
                connection.sendContentHeader((short) 1, (short) 60, 1000L, null)
            ).doesNotThrowAnyException();

            verify(nettyChannel).writeAndFlush(any(AmqpFrame.class));
        }

        @Test
        @DisplayName("Should handle sending content body")
        void testSendContentBody() {
            ByteBuf body = Unpooled.wrappedBuffer("message body".getBytes());

            assertThatCode(() ->
                connection.sendContentBody((short) 1, body)
            ).doesNotThrowAnyException();

            verify(nettyChannel).writeAndFlush(any(AmqpFrame.class));
        }

        @Test
        @DisplayName("Should handle sending empty content body")
        void testSendEmptyContentBody() {
            ByteBuf body = Unpooled.buffer(0);

            assertThatCode(() ->
                connection.sendContentBody((short) 1, body)
            ).doesNotThrowAnyException();

            verify(nettyChannel).writeAndFlush(any(AmqpFrame.class));
        }
    }

    @Nested
    @DisplayName("Connection State Tests")
    class ConnectionStateTests {

        @Test
        @DisplayName("Should handle closing already closed connection")
        void testDoubleClose() {
            connection.close();

            // Second close should not throw
            assertThatCode(() -> connection.close())
                .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should not be connected before explicit connection")
        void testInitialConnectionState() {
            assertThat(connection.isConnected()).isFalse();
        }

        @Test
        @DisplayName("Should handle connection state with inactive channel")
        void testConnectionStateInactiveChannel() {
            connection.setConnected(true);
            when(nettyChannel.isActive()).thenReturn(false);

            assertThat(connection.isConnected()).isFalse();
        }

        @Test
        @DisplayName("Should require both connected flag and active channel")
        void testConnectionStateRequirements() {
            // Connected but channel inactive
            connection.setConnected(true);
            when(nettyChannel.isActive()).thenReturn(false);
            assertThat(connection.isConnected()).isFalse();

            // Channel active but not connected
            connection.setConnected(false);
            when(nettyChannel.isActive()).thenReturn(true);
            assertThat(connection.isConnected()).isFalse();

            // Both required
            connection.setConnected(true);
            when(nettyChannel.isActive()).thenReturn(true);
            assertThat(connection.isConnected()).isTrue();
        }
    }

    @Nested
    @DisplayName("Virtual Host Tests")
    class VirtualHostTests {

        @Test
        @DisplayName("Should default to root virtual host")
        void testDefaultVirtualHost() {
            assertThat(connection.getVirtualHost()).isEqualTo("/");
        }

        @Test
        @DisplayName("Should accept custom virtual host")
        void testCustomVirtualHost() {
            connection.setVirtualHost("/custom");
            assertThat(connection.getVirtualHost()).isEqualTo("/custom");

            connection.setVirtualHost("/vhost1");
            assertThat(connection.getVirtualHost()).isEqualTo("/vhost1");
        }

        @Test
        @DisplayName("Should accept empty virtual host")
        void testEmptyVirtualHost() {
            connection.setVirtualHost("");
            assertThat(connection.getVirtualHost()).isEmpty();
        }

        @Test
        @DisplayName("Should accept null virtual host")
        void testNullVirtualHost() {
            connection.setVirtualHost(null);
            assertThat(connection.getVirtualHost()).isNull();
        }
    }

    @Nested
    @DisplayName("Username Tests")
    class UsernameTests {

        @Test
        @DisplayName("Should default to null username")
        void testDefaultUsername() {
            assertThat(connection.getUsername()).isNull();
        }

        @Test
        @DisplayName("Should accept username")
        void testSetUsername() {
            connection.setUsername("admin");
            assertThat(connection.getUsername()).isEqualTo("admin");

            connection.setUsername("guest");
            assertThat(connection.getUsername()).isEqualTo("guest");
        }

        @Test
        @DisplayName("Should accept empty username")
        void testEmptyUsername() {
            connection.setUsername("");
            assertThat(connection.getUsername()).isEmpty();
        }

        @Test
        @DisplayName("Should accept null username")
        void testNullUsername() {
            connection.setUsername(null);
            assertThat(connection.getUsername()).isNull();
        }
    }

    @Nested
    @DisplayName("Broker Access Tests")
    class BrokerAccessTests {

        @Test
        @DisplayName("Should provide access to broker")
        void testGetBroker() {
            assertThat(connection.getBroker()).isEqualTo(broker);
        }

        @Test
        @DisplayName("Should maintain broker reference through connection lifecycle")
        void testBrokerReferenceStability() {
            AmqpBroker initialBroker = connection.getBroker();

            connection.setConnected(true);
            assertThat(connection.getBroker()).isSameAs(initialBroker);

            connection.openChannel((short) 1);
            assertThat(connection.getBroker()).isSameAs(initialBroker);

            connection.close();
            assertThat(connection.getBroker()).isSameAs(initialBroker);
        }
    }

    @Nested
    @DisplayName("Stress and Performance Tests")
    class StressTests {

        @Test
        @DisplayName("Should handle rapid channel open and close cycles")
        void testRapidChannelCycles() {
            for (int i = 0; i < 100; i++) {
                connection.openChannel((short) 1);
                connection.closeChannel((short) 1);
            }
        }

        @Test
        @DisplayName("Should handle many sequential frame sends")
        void testManyFrameSends() {
            for (int i = 0; i < 1000; i++) {
                ByteBuf payload = Unpooled.buffer();
                payload.writeInt(i);
                connection.sendMethod((short) 0, (short) 10, (short) 20, payload);
            }

            verify(nettyChannel, times(1000)).writeAndFlush(any(AmqpFrame.class));
        }

        @Test
        @DisplayName("Should handle frame for each channel number")
        void testFramesForAllChannels() {
            for (short i = 1; i <= 10; i++) {
                connection.openChannel(i);
            }

            for (short i = 1; i <= 10; i++) {
                ByteBuf payload = Unpooled.buffer();
                payload.writeShort(20);
                payload.writeShort(10);
                AmqpFrame frame = new AmqpFrame((byte) 1, i, payload);
                connection.handleFrame(frame);
            }
        }
    }
}
