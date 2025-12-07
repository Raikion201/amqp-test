package com.amqp.amqp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import static org.assertj.core.api.Assertions.*;

@DisplayName("AMQP Frame Tests")
class AmqpFrameTest {

    @Nested
    @DisplayName("Frame Construction Tests")
    class FrameConstructionTests {

        @Test
        @DisplayName("Should create frame with correct properties")
        void testFrameCreation() {
            ByteBuf payload = Unpooled.wrappedBuffer("test payload".getBytes());
            AmqpFrame frame = new AmqpFrame((byte) 1, (short) 2, payload);

            assertThat(frame.getType()).isEqualTo((byte) 1);
            assertThat(frame.getChannel()).isEqualTo((short) 2);
            assertThat(frame.getSize()).isEqualTo("test payload".length());
            assertThat(frame.getPayload()).isEqualTo(payload);
        }

        @Test
        @DisplayName("Should create frame with empty payload")
        void testFrameWithEmptyPayload() {
            ByteBuf payload = Unpooled.buffer(0);
            AmqpFrame frame = new AmqpFrame((byte) 1, (short) 0, payload);

            assertThat(frame.getSize()).isEqualTo(0);
            assertThat(frame.getPayload().readableBytes()).isEqualTo(0);
        }

        @Test
        @DisplayName("Should create frame with large payload")
        void testFrameWithLargePayload() {
            byte[] largeData = new byte[131072]; // 128 KB
            ByteBuf payload = Unpooled.wrappedBuffer(largeData);
            AmqpFrame frame = new AmqpFrame((byte) 3, (short) 1, payload);

            assertThat(frame.getSize()).isEqualTo(131072);
        }

        @Test
        @DisplayName("Should create frame with maximum channel number")
        void testFrameWithMaxChannelNumber() {
            ByteBuf payload = Unpooled.wrappedBuffer("data".getBytes());
            AmqpFrame frame = new AmqpFrame((byte) 1, Short.MAX_VALUE, payload);

            assertThat(frame.getChannel()).isEqualTo(Short.MAX_VALUE);
        }

        @Test
        @DisplayName("Should create frame with channel 0")
        void testFrameWithChannelZero() {
            ByteBuf payload = Unpooled.wrappedBuffer("data".getBytes());
            AmqpFrame frame = new AmqpFrame((byte) 1, (short) 0, payload);

            assertThat(frame.getChannel()).isEqualTo((short) 0);
        }
    }

    @Nested
    @DisplayName("Frame Constants Tests")
    class FrameConstantsTests {

        @Test
        @DisplayName("Should have correct frame header size constant")
        void testFrameHeaderSize() {
            assertThat(AmqpFrame.FRAME_HEADER_SIZE).isEqualTo(7);
        }

        @Test
        @DisplayName("Should have correct frame end size constant")
        void testFrameEndSize() {
            assertThat(AmqpFrame.FRAME_END_SIZE).isEqualTo(1);
        }

        @Test
        @DisplayName("Should have correct frame end marker")
        void testFrameEndMarker() {
            assertThat(AmqpFrame.FRAME_END).isEqualTo((byte) 0xCE);
        }
    }

    @Nested
    @DisplayName("FrameType Enum Tests")
    class FrameTypeEnumTests {

        @Test
        @DisplayName("Should have correct METHOD frame type value")
        void testMethodFrameType() {
            assertThat(AmqpFrame.FrameType.METHOD.getValue()).isEqualTo((byte) 1);
        }

        @Test
        @DisplayName("Should have correct HEADER frame type value")
        void testHeaderFrameType() {
            assertThat(AmqpFrame.FrameType.HEADER.getValue()).isEqualTo((byte) 2);
        }

        @Test
        @DisplayName("Should have correct BODY frame type value")
        void testBodyFrameType() {
            assertThat(AmqpFrame.FrameType.BODY.getValue()).isEqualTo((byte) 3);
        }

        @Test
        @DisplayName("Should have correct HEARTBEAT frame type value")
        void testHeartbeatFrameType() {
            assertThat(AmqpFrame.FrameType.HEARTBEAT.getValue()).isEqualTo((byte) 8);
        }

        @Test
        @DisplayName("Should convert byte value to METHOD frame type")
        void testFromValueMethod() {
            AmqpFrame.FrameType frameType = AmqpFrame.FrameType.fromValue((byte) 1);
            assertThat(frameType).isEqualTo(AmqpFrame.FrameType.METHOD);
        }

        @Test
        @DisplayName("Should convert byte value to HEADER frame type")
        void testFromValueHeader() {
            AmqpFrame.FrameType frameType = AmqpFrame.FrameType.fromValue((byte) 2);
            assertThat(frameType).isEqualTo(AmqpFrame.FrameType.HEADER);
        }

        @Test
        @DisplayName("Should convert byte value to BODY frame type")
        void testFromValueBody() {
            AmqpFrame.FrameType frameType = AmqpFrame.FrameType.fromValue((byte) 3);
            assertThat(frameType).isEqualTo(AmqpFrame.FrameType.BODY);
        }

        @Test
        @DisplayName("Should convert byte value to HEARTBEAT frame type")
        void testFromValueHeartbeat() {
            AmqpFrame.FrameType frameType = AmqpFrame.FrameType.fromValue((byte) 8);
            assertThat(frameType).isEqualTo(AmqpFrame.FrameType.HEARTBEAT);
        }

        @Test
        @DisplayName("Should throw exception for invalid frame type value")
        void testFromValueInvalid() {
            assertThatThrownBy(() -> AmqpFrame.FrameType.fromValue((byte) 99))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown frame type: 99");
        }

        @Test
        @DisplayName("Should throw exception for negative frame type value")
        void testFromValueNegative() {
            assertThatThrownBy(() -> AmqpFrame.FrameType.fromValue((byte) -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown frame type");
        }

        @Test
        @DisplayName("Should throw exception for frame type value 0")
        void testFromValueZero() {
            assertThatThrownBy(() -> AmqpFrame.FrameType.fromValue((byte) 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown frame type: 0");
        }

        @Test
        @DisplayName("Should have all frame types enumerated")
        void testAllFrameTypes() {
            AmqpFrame.FrameType[] types = AmqpFrame.FrameType.values();
            assertThat(types).hasSize(4);
            assertThat(types).contains(
                AmqpFrame.FrameType.METHOD,
                AmqpFrame.FrameType.HEADER,
                AmqpFrame.FrameType.BODY,
                AmqpFrame.FrameType.HEARTBEAT
            );
        }
    }

    @Nested
    @DisplayName("Frame Type Usage Tests")
    class FrameTypeUsageTests {

        @Test
        @DisplayName("Should create METHOD frame using FrameType enum")
        void testCreateMethodFrame() {
            ByteBuf payload = Unpooled.wrappedBuffer("method".getBytes());
            AmqpFrame frame = new AmqpFrame(
                AmqpFrame.FrameType.METHOD.getValue(),
                (short) 1,
                payload
            );

            assertThat(frame.getType()).isEqualTo(AmqpFrame.FrameType.METHOD.getValue());
        }

        @Test
        @DisplayName("Should create HEADER frame using FrameType enum")
        void testCreateHeaderFrame() {
            ByteBuf payload = Unpooled.wrappedBuffer("header".getBytes());
            AmqpFrame frame = new AmqpFrame(
                AmqpFrame.FrameType.HEADER.getValue(),
                (short) 1,
                payload
            );

            assertThat(frame.getType()).isEqualTo(AmqpFrame.FrameType.HEADER.getValue());
        }

        @Test
        @DisplayName("Should create BODY frame using FrameType enum")
        void testCreateBodyFrame() {
            ByteBuf payload = Unpooled.wrappedBuffer("body".getBytes());
            AmqpFrame frame = new AmqpFrame(
                AmqpFrame.FrameType.BODY.getValue(),
                (short) 1,
                payload
            );

            assertThat(frame.getType()).isEqualTo(AmqpFrame.FrameType.BODY.getValue());
        }

        @Test
        @DisplayName("Should create HEARTBEAT frame using FrameType enum")
        void testCreateHeartbeatFrame() {
            ByteBuf payload = Unpooled.buffer(0);
            AmqpFrame frame = new AmqpFrame(
                AmqpFrame.FrameType.HEARTBEAT.getValue(),
                (short) 0,
                payload
            );

            assertThat(frame.getType()).isEqualTo(AmqpFrame.FrameType.HEARTBEAT.getValue());
        }
    }

    @Nested
    @DisplayName("Payload Handling Tests")
    class PayloadHandlingTests {

        @Test
        @DisplayName("Should calculate size from payload readable bytes")
        void testSizeFromPayload() {
            ByteBuf payload = Unpooled.wrappedBuffer("test data".getBytes());
            AmqpFrame frame = new AmqpFrame((byte) 1, (short) 0, payload);

            assertThat(frame.getSize()).isEqualTo(payload.readableBytes());
        }

        @Test
        @DisplayName("Should maintain payload reference")
        void testPayloadReference() {
            ByteBuf payload = Unpooled.wrappedBuffer("original".getBytes());
            AmqpFrame frame = new AmqpFrame((byte) 1, (short) 0, payload);

            assertThat(frame.getPayload()).isSameAs(payload);
        }

        @Test
        @DisplayName("Should handle payload with different reader indices")
        void testPayloadWithReaderIndex() {
            ByteBuf payload = Unpooled.wrappedBuffer("0123456789".getBytes());
            payload.readerIndex(3); // Skip first 3 bytes

            AmqpFrame frame = new AmqpFrame((byte) 1, (short) 0, payload);

            assertThat(frame.getSize()).isEqualTo(7); // 10 - 3 = 7 readable bytes
        }

        @Test
        @DisplayName("Should handle ByteBuf with capacity larger than readable bytes")
        void testPayloadCapacityVsReadable() {
            ByteBuf payload = Unpooled.buffer(100);
            payload.writeBytes("data".getBytes());

            AmqpFrame frame = new AmqpFrame((byte) 1, (short) 0, payload);

            assertThat(frame.getSize()).isEqualTo(4); // Only readable bytes count
        }
    }
}
