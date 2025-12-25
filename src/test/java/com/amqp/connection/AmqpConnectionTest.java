package com.amqp.connection;

import com.amqp.amqp.AmqpFrame;
import com.amqp.server.AmqpBroker;
import com.amqp.persistence.DatabaseManager;
import com.amqp.persistence.PersistenceManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.*;

@DisplayName("AMQP Connection Tests")
class AmqpConnectionTest {

    private EmbeddedChannel nettyChannel;
    private AmqpBroker broker;
    private AmqpConnection connection;

    @BeforeEach
    void setUp() {
        nettyChannel = new EmbeddedChannel();
        // Create a real broker with in-memory H2 database
        DatabaseManager dbManager = new DatabaseManager(
            "jdbc:h2:mem:test_" + System.currentTimeMillis() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
            "sa", "");
        PersistenceManager persistenceManager = new PersistenceManager(dbManager);
        broker = new AmqpBroker(persistenceManager, true);
        connection = new AmqpConnection(nettyChannel, broker);
    }

    @AfterEach
    void tearDown() {
        if (nettyChannel != null && nettyChannel.isOpen()) {
            nettyChannel.close();
        }
        if (broker != null) {
            broker.stop();
        }
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

        // Verify frame was written to the channel
        AmqpFrame sentFrame = nettyChannel.readOutbound();
        assertThat(sentFrame).isEqualTo(frame);
    }

    @Test
    @DisplayName("Should not send frame when channel is inactive")
    void testSendFrameInactiveChannel() {
        // Close the channel to make it inactive
        nettyChannel.close();

        ByteBuf payload = Unpooled.buffer();
        AmqpFrame frame = new AmqpFrame((byte) 1, (short) 0, payload);

        connection.sendFrame(frame);

        // No frame should be written
        assertThat((Object) nettyChannel.readOutbound()).isNull();
    }

    @Test
    @DisplayName("Should send method frame")
    void testSendMethod() {
        ByteBuf payload = Unpooled.buffer();
        payload.writeInt(123);

        connection.sendMethod((short) 1, (short) 10, (short) 20, payload);

        // Verify frame was written
        AmqpFrame sentFrame = nettyChannel.readOutbound();
        assertThat(sentFrame).isNotNull();
        assertThat(sentFrame.getType()).isEqualTo(AmqpFrame.FrameType.METHOD.getValue());
    }

    @Test
    @DisplayName("Should send method frame with null payload")
    void testSendMethodNullPayload() {
        connection.sendMethod((short) 1, (short) 10, (short) 20, null);

        // Verify frame was written
        AmqpFrame sentFrame = nettyChannel.readOutbound();
        assertThat(sentFrame).isNotNull();
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
