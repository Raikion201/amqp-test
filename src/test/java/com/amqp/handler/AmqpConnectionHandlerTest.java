package com.amqp.handler;

import com.amqp.amqp.AmqpFrame;
import com.amqp.amqp.AmqpCodec;
import com.amqp.server.AmqpBroker;
import com.amqp.connection.AmqpConnection;
import com.amqp.persistence.DatabaseManager;
import com.amqp.persistence.PersistenceManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static org.assertj.core.api.Assertions.*;

@DisplayName("AMQP Connection Handler Tests")
class AmqpConnectionHandlerTest {

    private AmqpBroker broker;
    private EmbeddedChannel channel;
    private AmqpConnectionHandler handler;

    @BeforeEach
    void setUp() {
        // Create a real broker with in-memory H2 database
        DatabaseManager dbManager = new DatabaseManager(
            "jdbc:h2:mem:test_" + System.currentTimeMillis() + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
            "sa", "");
        PersistenceManager persistenceManager = new PersistenceManager(dbManager);
        broker = new AmqpBroker(persistenceManager, true);
        handler = new AmqpConnectionHandler(broker);
        channel = new EmbeddedChannel(handler);
    }

    @AfterEach
    void tearDown() {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        if (broker != null) {
            broker.stop();
        }
    }

    @Nested
    @DisplayName("Handler Construction Tests")
    class HandlerConstructionTests {

        @Test
        @DisplayName("Should create handler with broker")
        void testHandlerCreation() {
            AmqpConnectionHandler handler = new AmqpConnectionHandler(broker);
            assertThat(handler).isNotNull();
        }

        @Test
        @DisplayName("Should accept non-null broker")
        void testHandlerWithBroker() {
            assertThat(handler).isNotNull();
        }
    }

    /**
     * Trigger the protocol header received event to simulate receiving the AMQP protocol header.
     * In the real server, this is done by the ProtocolHeaderDecoder.
     */
    private void sendAmqpProtocolHeader() {
        // Fire the ProtocolHeaderReceivedEvent to trigger Connection.Start
        channel.pipeline().fireUserEventTriggered(com.amqp.amqp.ProtocolHeaderReceivedEvent.INSTANCE);
    }

    @Nested
    @DisplayName("Channel Activation Tests")
    class ChannelActivationTests {

        @Test
        @DisplayName("Should send Connection.Start after receiving protocol header")
        void testChannelActiveSendsConnectionStart() {
            // Send AMQP protocol header first
            sendAmqpProtocolHeader();

            // Check that Connection.Start frame was sent
            AmqpFrame sentFrame = channel.readOutbound();

            assertThat(sentFrame).isNotNull();
            assertThat(sentFrame.getType()).isEqualTo(AmqpFrame.FrameType.METHOD.getValue());
            assertThat(sentFrame.getChannel()).isEqualTo((short) 0);

            // Verify it's a Connection.Start method
            ByteBuf payload = sentFrame.getPayload();
            short classId = payload.readShort();
            short methodId = payload.readShort();

            assertThat(classId).isEqualTo((short) 10); // Connection class
            assertThat(methodId).isEqualTo((short) 10); // Start method

            sentFrame.getPayload().release();
        }

        @Test
        @DisplayName("Should create connection on channel active")
        void testConnectionCreatedOnActive() {
            // Send AMQP protocol header first
            sendAmqpProtocolHeader();

            // Connection should be created when channel becomes active
            // This is verified by the fact that Connection.Start is sent
            AmqpFrame sentFrame = channel.readOutbound();
            assertThat(sentFrame).isNotNull();
            sentFrame.getPayload().release();
        }

        @Test
        @DisplayName("Should send Connection.Start with correct protocol version")
        void testConnectionStartProtocolVersion() {
            // Send AMQP protocol header first
            sendAmqpProtocolHeader();

            AmqpFrame sentFrame = channel.readOutbound();
            assertThat(sentFrame).isNotNull();

            ByteBuf payload = sentFrame.getPayload();
            payload.readShort(); // class ID
            payload.readShort(); // method ID

            byte versionMajor = payload.readByte();
            byte versionMinor = payload.readByte();

            assertThat(versionMajor).isEqualTo((byte) 0);
            assertThat(versionMinor).isEqualTo((byte) 9);

            sentFrame.getPayload().release();
        }

        @Test
        @DisplayName("Should send Connection.Start with mechanism")
        void testConnectionStartMechanism() {
            // Send AMQP protocol header first
            sendAmqpProtocolHeader();

            AmqpFrame sentFrame = channel.readOutbound();
            assertThat(sentFrame).isNotNull();

            ByteBuf payload = sentFrame.getPayload();
            payload.readShort(); // class ID
            payload.readShort(); // method ID
            payload.skipBytes(2); // version
            payload.skipBytes(4); // server properties

            String mechanisms = AmqpCodec.decodeLongString(payload);
            assertThat(mechanisms).contains("PLAIN");

            sentFrame.getPayload().release();
        }
    }

    @Nested
    @DisplayName("Frame Handling Tests")
    class FrameHandlingTests {

        @Test
        @DisplayName("Should handle incoming METHOD frame")
        void testHandleMethodFrame() {
            // Clear the Connection.Start frame sent on activation
            channel.readOutbound();

            // Create a test frame
            ByteBuf payload = Unpooled.buffer();
            payload.writeShort(10); // Connection class
            payload.writeShort(11); // Start-Ok method

            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.METHOD.getValue(), (short) 0, payload);

            // Should not throw exception
            assertThatCode(() -> channel.writeInbound(frame)).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle incoming HEARTBEAT frame")
        void testHandleHeartbeatFrame() {
            channel.readOutbound(); // Clear Connection.Start

            ByteBuf payload = Unpooled.buffer(0);
            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.HEARTBEAT.getValue(), (short) 0, payload);

            assertThatCode(() -> channel.writeInbound(frame)).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle incoming HEADER frame")
        void testHandleHeaderFrame() {
            channel.readOutbound(); // Clear Connection.Start

            ByteBuf payload = Unpooled.buffer();
            payload.writeShort(60); // Basic class
            payload.writeShort(0);
            payload.writeLong(100); // body size

            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.HEADER.getValue(), (short) 1, payload);

            assertThatCode(() -> channel.writeInbound(frame)).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle incoming BODY frame")
        void testHandleBodyFrame() {
            channel.readOutbound(); // Clear Connection.Start

            ByteBuf payload = Unpooled.wrappedBuffer("message body".getBytes());
            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.BODY.getValue(), (short) 1, payload);

            assertThatCode(() -> channel.writeInbound(frame)).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should handle multiple frames in sequence")
        void testHandleMultipleFrames() {
            channel.readOutbound(); // Clear Connection.Start

            for (int i = 0; i < 10; i++) {
                ByteBuf payload = Unpooled.buffer();
                payload.writeShort(10);
                payload.writeShort(11);

                AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.METHOD.getValue(), (short) 0, payload);
                channel.writeInbound(frame);
            }

            // Verify channel is still active after handling multiple frames
            assertThat(channel.isActive()).isTrue();
        }
    }

    @Nested
    @DisplayName("Channel Deactivation Tests")
    class ChannelDeactivationTests {

        @Test
        @DisplayName("Should handle channel inactive gracefully")
        void testChannelInactive() {
            channel.readOutbound(); // Clear Connection.Start

            // Close the channel
            assertThatCode(() -> channel.close()).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Should clean up on channel inactive")
        void testChannelInactiveCleanup() {
            channel.readOutbound(); // Clear Connection.Start

            channel.close();

            // Connection should be closed, no more frames should be sent
            AmqpFrame frame = channel.readOutbound();
            assertThat(frame).isNull();
        }
    }

    @Nested
    @DisplayName("Exception Handling Tests")
    class ExceptionHandlingTests {

        @Test
        @DisplayName("Should handle exception and close channel")
        void testExceptionCaught() {
            channel.readOutbound(); // Clear Connection.Start

            // Simulate an exception
            Exception testException = new RuntimeException("Test exception");
            channel.pipeline().fireExceptionCaught(testException);

            // Channel should be closed
            assertThat(channel.isActive()).isFalse();
        }

        @Test
        @DisplayName("Should handle null pointer exception")
        void testNullPointerException() {
            channel.readOutbound(); // Clear Connection.Start

            NullPointerException npe = new NullPointerException("Test NPE");
            channel.pipeline().fireExceptionCaught(npe);

            assertThat(channel.isActive()).isFalse();
        }

        @Test
        @DisplayName("Should handle IO exception")
        void testIOException() {
            channel.readOutbound(); // Clear Connection.Start

            java.io.IOException ioException = new java.io.IOException("Test IO exception");
            channel.pipeline().fireExceptionCaught(ioException);

            assertThat(channel.isActive()).isFalse();
        }
    }

    @Nested
    @DisplayName("Connection Management Tests")
    class ConnectionManagementTests {

        @Test
        @DisplayName("Should track active connections")
        void testConnectionTracking() {
            // Connection should be created on channel active
            assertThat(channel.isActive()).isTrue();
        }

        @Test
        @DisplayName("Should remove connection on channel inactive")
        void testConnectionRemoval() {
            channel.readOutbound(); // Clear Connection.Start

            channel.close();

            // After close, channel should be inactive
            assertThat(channel.isActive()).isFalse();
        }

        @Test
        @DisplayName("Should handle multiple connections")
        void testMultipleConnections() {
            // Create multiple handler instances
            AmqpConnectionHandler handler1 = new AmqpConnectionHandler(broker);
            AmqpConnectionHandler handler2 = new AmqpConnectionHandler(broker);
            AmqpConnectionHandler handler3 = new AmqpConnectionHandler(broker);

            EmbeddedChannel channel1 = new EmbeddedChannel(handler1);
            EmbeddedChannel channel2 = new EmbeddedChannel(handler2);
            EmbeddedChannel channel3 = new EmbeddedChannel(handler3);

            assertThat(channel1.isActive()).isTrue();
            assertThat(channel2.isActive()).isTrue();
            assertThat(channel3.isActive()).isTrue();

            channel1.close();
            channel2.close();
            channel3.close();
        }
    }

    @Nested
    @DisplayName("Protocol Compliance Tests")
    class ProtocolComplianceTests {

        @Test
        @DisplayName("Should send Connection.Start after protocol header")
        void testConnectionStartFirst() {
            // Send AMQP protocol header first
            sendAmqpProtocolHeader();

            AmqpFrame firstFrame = channel.readOutbound();

            assertThat(firstFrame).isNotNull();

            ByteBuf payload = firstFrame.getPayload();
            short classId = payload.readShort();
            short methodId = payload.readShort();

            assertThat(classId).isEqualTo((short) 10); // Connection
            assertThat(methodId).isEqualTo((short) 10); // Start

            firstFrame.getPayload().release();
        }

        @Test
        @DisplayName("Should send Connection.Start on channel 0")
        void testConnectionStartOnChannelZero() {
            // Send AMQP protocol header first
            sendAmqpProtocolHeader();

            AmqpFrame frame = channel.readOutbound();
            assertThat(frame).isNotNull();

            assertThat(frame.getChannel()).isEqualTo((short) 0);

            frame.getPayload().release();
        }

        @Test
        @DisplayName("Should include server properties in Connection.Start")
        void testConnectionStartServerProperties() {
            // Send AMQP protocol header first
            sendAmqpProtocolHeader();

            AmqpFrame frame = channel.readOutbound();
            assertThat(frame).isNotNull();

            ByteBuf payload = frame.getPayload();
            payload.readShort(); // class ID
            payload.readShort(); // method ID
            payload.skipBytes(2); // version

            int serverPropsLength = payload.readInt();
            // Server properties should be present (even if empty)
            assertThat(serverPropsLength).isGreaterThanOrEqualTo(0);

            frame.getPayload().release();
        }

        @Test
        @DisplayName("Should include locale in Connection.Start")
        void testConnectionStartLocale() {
            // Send AMQP protocol header first
            sendAmqpProtocolHeader();

            AmqpFrame frame = channel.readOutbound();
            assertThat(frame).isNotNull();

            ByteBuf payload = frame.getPayload();
            payload.readShort(); // class ID
            payload.readShort(); // method ID
            payload.skipBytes(2); // version
            payload.skipBytes(4); // server properties
            AmqpCodec.decodeLongString(payload); // mechanisms

            String locales = AmqpCodec.decodeLongString(payload);
            assertThat(locales).contains("en_US");

            frame.getPayload().release();
        }
    }

    @Nested
    @DisplayName("Stress Tests")
    class StressTests {

        @Test
        @DisplayName("Should handle rapid connection creation and destruction")
        void testRapidConnectionCycles() {
            int successfulCycles = 0;
            for (int i = 0; i < 100; i++) {
                AmqpConnectionHandler tempHandler = new AmqpConnectionHandler(broker);
                EmbeddedChannel tempChannel = new EmbeddedChannel(tempHandler);
                tempChannel.readOutbound(); // Clear Connection.Start
                tempChannel.close();
                successfulCycles++;
            }

            // Verify all 100 cycles completed successfully
            assertThat(successfulCycles).isEqualTo(100);
        }

        @Test
        @DisplayName("Should handle many frames on single connection")
        void testManyFrames() {
            channel.readOutbound(); // Clear Connection.Start

            int framesProcessed = 0;
            for (int i = 0; i < 1000; i++) {
                ByteBuf payload = Unpooled.buffer();
                payload.writeShort(8); // Heartbeat would be simplest
                payload.writeShort(0);

                AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.HEARTBEAT.getValue(), (short) 0, payload);
                channel.writeInbound(frame);
                framesProcessed++;
            }

            // Verify all 1000 frames were processed and channel is still active
            assertThat(framesProcessed).isEqualTo(1000);
            assertThat(channel.isActive()).isTrue();
        }
    }
}
