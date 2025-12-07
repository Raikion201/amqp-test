package com.amqp.connection;

import com.amqp.amqp.AmqpFrame;
import com.amqp.server.AmqpBroker;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("AMQP Connection Tests")
class AmqpConnectionTest {

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

    @Test
    @DisplayName("Should create connection with main channel (0)")
    void testConnectionCreation() {
        assertThat(connection).isNotNull();
        assertThat(connection.getVirtualHost()).isEqualTo("/");
    }

    @Test
    @DisplayName("Should open new channel")
    void testOpenChannel() {
        AmqpChannel channel = connection.openChannel((short) 1);

        assertThat(channel).isNotNull();
        assertThat(channel.getChannelNumber()).isEqualTo((short) 1);
    }

    @Test
    @DisplayName("Should throw exception when opening channel with existing number")
    void testOpenDuplicateChannel() {
        connection.openChannel((short) 1);

        assertThatThrownBy(() -> connection.openChannel((short) 1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Channel already exists");
    }

    @Test
    @DisplayName("Should close channel")
    void testCloseChannel() {
        connection.openChannel((short) 1);
        
        assertThatCode(() -> connection.closeChannel((short) 1))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle closing non-existent channel gracefully")
    void testCloseNonExistentChannel() {
        assertThatCode(() -> connection.closeChannel((short) 99))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should send frame when channel is active")
    void testSendFrame() {
        ByteBuf payload = Unpooled.buffer();
        payload.writeInt(12345);
        AmqpFrame frame = new AmqpFrame((byte) 1, (short) 0, payload);

        connection.sendFrame(frame);

        verify(nettyChannel).writeAndFlush(frame);
    }

    @Test
    @DisplayName("Should not send frame when channel is inactive")
    void testSendFrameInactiveChannel() {
        when(nettyChannel.isActive()).thenReturn(false);
        
        ByteBuf payload = Unpooled.buffer();
        AmqpFrame frame = new AmqpFrame((byte) 1, (short) 0, payload);

        connection.sendFrame(frame);

        verify(nettyChannel, never()).writeAndFlush(any());
    }

    @Test
    @DisplayName("Should send method frame")
    void testSendMethod() {
        ByteBuf payload = Unpooled.buffer();
        payload.writeInt(123);

        connection.sendMethod((short) 1, (short) 10, (short) 20, payload);

        verify(nettyChannel).writeAndFlush(any(AmqpFrame.class));
    }

    @Test
    @DisplayName("Should send method frame with null payload")
    void testSendMethodNullPayload() {
        connection.sendMethod((short) 1, (short) 10, (short) 20, null);

        verify(nettyChannel).writeAndFlush(any(AmqpFrame.class));
    }

    @Test
    @DisplayName("Should handle frame on correct channel")
    void testHandleFrame() {
        AmqpChannel channel = connection.openChannel((short) 1);
        
        ByteBuf payload = Unpooled.buffer();
        payload.writeShort(20); // class id
        payload.writeShort(10); // method id
        AmqpFrame frame = new AmqpFrame((byte) 1, (short) 1, payload);

        assertThatCode(() -> connection.handleFrame(frame))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should set virtual host")
    void testSetVirtualHost() {
        connection.setVirtualHost("/custom");

        assertThat(connection.getVirtualHost()).isEqualTo("/custom");
    }

    @Test
    @DisplayName("Should set username")
    void testSetUsername() {
        connection.setUsername("admin");

        assertThat(connection.getUsername()).isEqualTo("admin");
    }

    @Test
    @DisplayName("Should handle connected state")
    void testConnectedState() {
        assertThat(connection.isConnected()).isFalse();
        
        connection.setConnected(true);
        assertThat(connection.isConnected()).isTrue();
        
        connection.setConnected(false);
        assertThat(connection.isConnected()).isFalse();
    }

    @Test
    @DisplayName("Should support multiple channels per connection")
    void testMultipleChannels() {
        AmqpChannel ch1 = connection.openChannel((short) 1);
        AmqpChannel ch2 = connection.openChannel((short) 2);
        AmqpChannel ch3 = connection.openChannel((short) 3);

        assertThat(ch1.getChannelNumber()).isEqualTo((short) 1);
        assertThat(ch2.getChannelNumber()).isEqualTo((short) 2);
        assertThat(ch3.getChannelNumber()).isEqualTo((short) 3);
    }

    @Test
    @DisplayName("Should close all channels when closing connection")
    void testCloseAllChannels() {
        connection.openChannel((short) 1);
        connection.openChannel((short) 2);
        connection.openChannel((short) 3);

        connection.close();

        // Verify channels are closed (attempting to open same channel should work)
        assertThatCode(() -> connection.openChannel((short) 1))
            .doesNotThrowAnyException();
    }
}
