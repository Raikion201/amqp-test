package com.amqp.amqp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("AMQP Codec Tests")
class AmqpCodecTest {

    @Nested
    @DisplayName("AmqpFrameEncoder Tests")
    class AmqpFrameEncoderTests {

        private EmbeddedChannel channel;

        @BeforeEach
        void setUp() {
            channel = new EmbeddedChannel(new AmqpCodec.AmqpFrameEncoder());
        }

        @Test
        @DisplayName("Should encode frame with correct format")
        void testEncodeFrame() {
            ByteBuf payload = Unpooled.wrappedBuffer("test".getBytes());
            AmqpFrame frame = new AmqpFrame((byte) 1, (short) 2, payload);

            channel.writeOutbound(frame);

            ByteBuf encoded = channel.readOutbound();

            assertThat(encoded.readByte()).isEqualTo((byte) 1); // type
            assertThat(encoded.readShort()).isEqualTo((short) 2); // channel
            assertThat(encoded.readInt()).isEqualTo(4); // size

            byte[] payloadBytes = new byte[4];
            encoded.readBytes(payloadBytes);
            assertThat(payloadBytes).isEqualTo("test".getBytes());

            assertThat(encoded.readByte()).isEqualTo(AmqpFrame.FRAME_END); // frame end marker

            encoded.release();
        }

        @Test
        @DisplayName("Should encode frame with empty payload")
        void testEncodeEmptyPayload() {
            ByteBuf payload = Unpooled.buffer(0);
            AmqpFrame frame = new AmqpFrame((byte) 8, (short) 0, payload);

            channel.writeOutbound(frame);

            ByteBuf encoded = channel.readOutbound();

            assertThat(encoded.readByte()).isEqualTo((byte) 8);
            assertThat(encoded.readShort()).isEqualTo((short) 0);
            assertThat(encoded.readInt()).isEqualTo(0); // empty payload
            assertThat(encoded.readByte()).isEqualTo(AmqpFrame.FRAME_END);

            encoded.release();
        }

        @Test
        @DisplayName("Should encode frame with large payload")
        void testEncodeLargePayload() {
            byte[] largeData = new byte[10000];
            for (int i = 0; i < largeData.length; i++) {
                largeData[i] = (byte) (i % 256);
            }

            ByteBuf payload = Unpooled.wrappedBuffer(largeData);
            AmqpFrame frame = new AmqpFrame((byte) 3, (short) 1, payload);

            channel.writeOutbound(frame);

            ByteBuf encoded = channel.readOutbound();

            assertThat(encoded.readByte()).isEqualTo((byte) 3);
            assertThat(encoded.readShort()).isEqualTo((short) 1);
            assertThat(encoded.readInt()).isEqualTo(10000);

            byte[] decodedPayload = new byte[10000];
            encoded.readBytes(decodedPayload);
            assertThat(decodedPayload).isEqualTo(largeData);

            assertThat(encoded.readByte()).isEqualTo(AmqpFrame.FRAME_END);

            encoded.release();
        }

        @Test
        @DisplayName("Should encode METHOD frame type correctly")
        void testEncodeMethodFrame() {
            ByteBuf payload = Unpooled.wrappedBuffer("method".getBytes());
            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.METHOD.getValue(), (short) 1, payload);

            channel.writeOutbound(frame);

            ByteBuf encoded = channel.readOutbound();
            assertThat(encoded.readByte()).isEqualTo((byte) 1);

            encoded.release();
        }

        @Test
        @DisplayName("Should encode HEARTBEAT frame type correctly")
        void testEncodeHeartbeatFrame() {
            ByteBuf payload = Unpooled.buffer(0);
            AmqpFrame frame = new AmqpFrame(AmqpFrame.FrameType.HEARTBEAT.getValue(), (short) 0, payload);

            channel.writeOutbound(frame);

            ByteBuf encoded = channel.readOutbound();
            assertThat(encoded.readByte()).isEqualTo((byte) 8);

            encoded.release();
        }
    }

    @Nested
    @DisplayName("AmqpFrameDecoder Tests")
    class AmqpFrameDecoderTests {

        private EmbeddedChannel channel;

        @BeforeEach
        void setUp() {
            channel = new EmbeddedChannel(new AmqpCodec.AmqpFrameDecoder());
        }

        @Test
        @DisplayName("Should decode valid frame")
        void testDecodeFrame() {
            ByteBuf input = Unpooled.buffer();
            input.writeByte(1); // type
            input.writeShort(2); // channel
            input.writeInt(4); // size
            input.writeBytes("test".getBytes()); // payload
            input.writeByte(AmqpFrame.FRAME_END); // frame end

            channel.writeInbound(input);

            AmqpFrame frame = channel.readInbound();

            assertThat(frame.getType()).isEqualTo((byte) 1);
            assertThat(frame.getChannel()).isEqualTo((short) 2);
            assertThat(frame.getSize()).isEqualTo(4);

            byte[] payload = new byte[4];
            frame.getPayload().readBytes(payload);
            assertThat(payload).isEqualTo("test".getBytes());

            frame.getPayload().release();
        }

        @Test
        @DisplayName("Should decode frame with empty payload")
        void testDecodeEmptyPayload() {
            ByteBuf input = Unpooled.buffer();
            input.writeByte(8); // heartbeat
            input.writeShort(0);
            input.writeInt(0); // empty
            input.writeByte(AmqpFrame.FRAME_END);

            channel.writeInbound(input);

            AmqpFrame frame = channel.readInbound();

            assertThat(frame.getType()).isEqualTo((byte) 8);
            assertThat(frame.getSize()).isEqualTo(0);

            frame.getPayload().release();
        }

        @Test
        @DisplayName("Should not decode incomplete frame")
        void testDecodeIncompleteFrame() {
            ByteBuf input = Unpooled.buffer();
            input.writeByte(1);
            input.writeShort(2);
            input.writeInt(100); // claims 100 bytes payload
            input.writeBytes("test".getBytes()); // only 4 bytes

            channel.writeInbound(input);

            AmqpFrame frame = channel.readInbound();
            assertThat(frame).isNull(); // Should not decode incomplete frame
        }

        @Test
        @DisplayName("Should not decode frame with insufficient header bytes")
        void testDecodeInsufficientHeader() {
            ByteBuf input = Unpooled.buffer();
            input.writeByte(1);
            input.writeShort(2);
            // Missing size and payload

            channel.writeInbound(input);

            AmqpFrame frame = channel.readInbound();
            assertThat(frame).isNull();
        }

        @Test
        @DisplayName("Should throw exception for invalid frame end marker")
        void testDecodeInvalidFrameEnd() {
            ByteBuf input = Unpooled.buffer();
            input.writeByte(1);
            input.writeShort(2);
            input.writeInt(4);
            input.writeBytes("test".getBytes());
            input.writeByte(0xFF); // Invalid frame end

            assertThatThrownBy(() -> channel.writeInbound(input))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid frame end marker");
        }

        @Test
        @DisplayName("Should decode multiple frames in sequence")
        void testDecodeMultipleFrames() {
            ByteBuf input = Unpooled.buffer();

            // Frame 1
            input.writeByte(1);
            input.writeShort(1);
            input.writeInt(3);
            input.writeBytes("abc".getBytes());
            input.writeByte(AmqpFrame.FRAME_END);

            // Frame 2
            input.writeByte(2);
            input.writeShort(2);
            input.writeInt(3);
            input.writeBytes("xyz".getBytes());
            input.writeByte(AmqpFrame.FRAME_END);

            channel.writeInbound(input);

            AmqpFrame frame1 = channel.readInbound();
            assertThat(frame1.getType()).isEqualTo((byte) 1);
            assertThat(frame1.getChannel()).isEqualTo((short) 1);

            AmqpFrame frame2 = channel.readInbound();
            assertThat(frame2.getType()).isEqualTo((byte) 2);
            assertThat(frame2.getChannel()).isEqualTo((short) 2);

            frame1.getPayload().release();
            frame2.getPayload().release();
        }

        @Test
        @DisplayName("Should handle frame decoding with partial data")
        void testDecodePartialData() {
            // Send first part of frame
            ByteBuf input1 = Unpooled.buffer();
            input1.writeByte(1);
            input1.writeShort(1);
            input1.writeInt(10);
            input1.writeBytes("test".getBytes()); // Only 4 of 10 bytes

            channel.writeInbound(input1);
            assertThat((AmqpFrame) channel.readInbound()).isNull();

            // Send remaining part
            ByteBuf input2 = Unpooled.buffer();
            input2.writeBytes("123456".getBytes()); // Remaining 6 bytes
            input2.writeByte(AmqpFrame.FRAME_END);

            channel.writeInbound(input2);

            AmqpFrame frame = channel.readInbound();
            assertThat(frame).isNotNull();
            assertThat(frame.getSize()).isEqualTo(10);

            frame.getPayload().release();
        }

        @Test
        @DisplayName("Should decode large frame correctly")
        void testDecodeLargeFrame() {
            byte[] largeData = new byte[50000];
            for (int i = 0; i < largeData.length; i++) {
                largeData[i] = (byte) (i % 256);
            }

            ByteBuf input = Unpooled.buffer();
            input.writeByte(3); // body frame
            input.writeShort(1);
            input.writeInt(50000);
            input.writeBytes(largeData);
            input.writeByte(AmqpFrame.FRAME_END);

            channel.writeInbound(input);

            AmqpFrame frame = channel.readInbound();
            assertThat(frame.getSize()).isEqualTo(50000);

            byte[] decoded = new byte[50000];
            frame.getPayload().readBytes(decoded);
            assertThat(decoded).isEqualTo(largeData);

            frame.getPayload().release();
        }
    }

    @Nested
    @DisplayName("ProtocolHeaderDecoder Tests")
    class ProtocolHeaderDecoderTests {

        private EmbeddedChannel channel;

        @BeforeEach
        void setUp() {
            channel = new EmbeddedChannel(new AmqpCodec.ProtocolHeaderDecoder());
        }

        @Test
        @DisplayName("Should decode valid AMQP protocol header")
        void testDecodeValidHeader() {
            ByteBuf input = Unpooled.buffer();
            input.writeBytes(new byte[]{'A', 'M', 'Q', 'P', 0, 0, 9, 1});

            channel.writeInbound(input);

            // Protocol header decoder should remove itself and add frame decoder
            ChannelPipeline pipeline = channel.pipeline();
            assertThat(pipeline.get(AmqpCodec.ProtocolHeaderDecoder.class)).isNull();
            assertThat(pipeline.get(AmqpCodec.AmqpFrameDecoder.class)).isNotNull();
        }

        @Test
        @DisplayName("Should throw exception for invalid protocol header")
        void testDecodeInvalidHeader() {
            ByteBuf input = Unpooled.buffer();
            input.writeBytes(new byte[]{'X', 'M', 'Q', 'P', 0, 0, 9, 1}); // Invalid first byte

            assertThatThrownBy(() -> channel.writeInbound(input))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid AMQP protocol header");
        }

        @Test
        @DisplayName("Should throw exception for wrong protocol version")
        void testDecodeWrongVersion() {
            ByteBuf input = Unpooled.buffer();
            input.writeBytes(new byte[]{'A', 'M', 'Q', 'P', 0, 0, 8, 0}); // Wrong version

            assertThatThrownBy(() -> channel.writeInbound(input))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid AMQP protocol header");
        }

        @Test
        @DisplayName("Should not decode with insufficient bytes")
        void testDecodeInsufficientBytes() {
            ByteBuf input = Unpooled.buffer();
            input.writeBytes(new byte[]{'A', 'M', 'Q', 'P'}); // Only 4 bytes

            channel.writeInbound(input);

            // Should wait for more data
            assertThat(channel.inboundMessages()).isEmpty();
        }

        @Test
        @DisplayName("Should handle header sent in multiple chunks")
        void testDecodeHeaderInChunks() {
            // Send first part
            ByteBuf input1 = Unpooled.buffer();
            input1.writeBytes(new byte[]{'A', 'M', 'Q'});
            channel.writeInbound(input1);

            // Send remaining part
            ByteBuf input2 = Unpooled.buffer();
            input2.writeBytes(new byte[]{'P', 0, 0, 9, 1});
            channel.writeInbound(input2);

            // Should have processed header successfully
            ChannelPipeline pipeline = channel.pipeline();
            assertThat(pipeline.get(AmqpCodec.ProtocolHeaderDecoder.class)).isNull();
            assertThat(pipeline.get(AmqpCodec.AmqpFrameDecoder.class)).isNotNull();
        }
    }

    @Nested
    @DisplayName("String Encoding/Decoding Tests")
    class StringCodecTests {

        @Test
        @DisplayName("Should encode and decode short string")
        void testShortString() {
            ByteBuf buf = Unpooled.buffer();
            String original = "test string";

            AmqpCodec.encodeShortString(buf, original);
            String decoded = AmqpCodec.decodeShortString(buf);

            assertThat(decoded).isEqualTo(original);
        }

        @Test
        @DisplayName("Should encode null short string as empty")
        void testShortStringNull() {
            ByteBuf buf = Unpooled.buffer();

            AmqpCodec.encodeShortString(buf, null);
            String decoded = AmqpCodec.decodeShortString(buf);

            assertThat(decoded).isEmpty();
        }

        @Test
        @DisplayName("Should encode empty short string")
        void testShortStringEmpty() {
            ByteBuf buf = Unpooled.buffer();

            AmqpCodec.encodeShortString(buf, "");
            String decoded = AmqpCodec.decodeShortString(buf);

            assertThat(decoded).isEmpty();
        }

        @Test
        @DisplayName("Should encode short string with special characters")
        void testShortStringSpecialChars() {
            ByteBuf buf = Unpooled.buffer();
            String original = "test@#$%^&*()";

            AmqpCodec.encodeShortString(buf, original);
            String decoded = AmqpCodec.decodeShortString(buf);

            assertThat(decoded).isEqualTo(original);
        }

        @Test
        @DisplayName("Should encode and decode long string")
        void testLongString() {
            ByteBuf buf = Unpooled.buffer();
            String original = "This is a much longer string that exceeds 255 characters and requires " +
                            "a different encoding format with a 4-byte length prefix instead of the " +
                            "1-byte prefix used for short strings in the AMQP protocol specification.";

            AmqpCodec.encodeLongString(buf, original);
            String decoded = AmqpCodec.decodeLongString(buf);

            assertThat(decoded).isEqualTo(original);
        }

        @Test
        @DisplayName("Should encode null long string as empty")
        void testLongStringNull() {
            ByteBuf buf = Unpooled.buffer();

            AmqpCodec.encodeLongString(buf, null);
            String decoded = AmqpCodec.decodeLongString(buf);

            assertThat(decoded).isEmpty();
        }

        @Test
        @DisplayName("Should encode empty long string")
        void testLongStringEmpty() {
            ByteBuf buf = Unpooled.buffer();

            AmqpCodec.encodeLongString(buf, "");
            String decoded = AmqpCodec.decodeLongString(buf);

            assertThat(decoded).isEmpty();
        }

        @Test
        @DisplayName("Should encode very long string")
        void testVeryLongString() {
            ByteBuf buf = Unpooled.buffer();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                sb.append("abcdefghij");
            }
            String original = sb.toString();

            AmqpCodec.encodeLongString(buf, original);
            String decoded = AmqpCodec.decodeLongString(buf);

            assertThat(decoded).isEqualTo(original);
            assertThat(decoded.length()).isEqualTo(10000);
        }

        @Test
        @DisplayName("Should encode string with unicode characters")
        void testUnicodeString() {
            ByteBuf buf = Unpooled.buffer();
            String original = "Hello 世界 🌍";

            AmqpCodec.encodeLongString(buf, original);
            String decoded = AmqpCodec.decodeLongString(buf);

            assertThat(decoded).isEqualTo(original);
        }

        @Test
        @DisplayName("Should correctly encode short string length")
        void testShortStringLength() {
            ByteBuf buf = Unpooled.buffer();
            String original = "test";

            AmqpCodec.encodeShortString(buf, original);

            buf.readerIndex(0);
            int length = buf.readUnsignedByte();
            assertThat(length).isEqualTo(4);
        }

        @Test
        @DisplayName("Should correctly encode long string length")
        void testLongStringLength() {
            ByteBuf buf = Unpooled.buffer();
            String original = "test string";

            AmqpCodec.encodeLongString(buf, original);

            buf.readerIndex(0);
            int length = buf.readInt();
            assertThat(length).isEqualTo(11);
        }
    }

    @Nested
    @DisplayName("Boolean Encoding/Decoding Tests")
    class BooleanCodecTests {

        @Test
        @DisplayName("Should encode true as 1")
        void testEncodeTrue() {
            ByteBuf buf = Unpooled.buffer();

            AmqpCodec.encodeBoolean(buf, true);

            buf.readerIndex(0);
            assertThat(buf.readByte()).isEqualTo((byte) 1);
        }

        @Test
        @DisplayName("Should encode false as 0")
        void testEncodeFalse() {
            ByteBuf buf = Unpooled.buffer();

            AmqpCodec.encodeBoolean(buf, false);

            buf.readerIndex(0);
            assertThat(buf.readByte()).isEqualTo((byte) 0);
        }

        @Test
        @DisplayName("Should decode 1 as true")
        void testDecodeTrue() {
            ByteBuf buf = Unpooled.buffer();
            buf.writeByte(1);

            boolean result = AmqpCodec.decodeBoolean(buf);

            assertThat(result).isTrue();
        }

        @Test
        @DisplayName("Should decode 0 as false")
        void testDecodeFalse() {
            ByteBuf buf = Unpooled.buffer();
            buf.writeByte(0);

            boolean result = AmqpCodec.decodeBoolean(buf);

            assertThat(result).isFalse();
        }

        @Test
        @DisplayName("Should decode non-zero as true")
        void testDecodeNonZeroAsTrue() {
            ByteBuf buf = Unpooled.buffer();
            buf.writeByte(42);

            boolean result = AmqpCodec.decodeBoolean(buf);

            assertThat(result).isTrue();
        }

        @Test
        @DisplayName("Should encode and decode true correctly")
        void testRoundTripTrue() {
            ByteBuf buf = Unpooled.buffer();

            AmqpCodec.encodeBoolean(buf, true);
            boolean decoded = AmqpCodec.decodeBoolean(buf);

            assertThat(decoded).isTrue();
        }

        @Test
        @DisplayName("Should encode and decode false correctly")
        void testRoundTripFalse() {
            ByteBuf buf = Unpooled.buffer();

            AmqpCodec.encodeBoolean(buf, false);
            boolean decoded = AmqpCodec.decodeBoolean(buf);

            assertThat(decoded).isFalse();
        }

        @Test
        @DisplayName("Should encode multiple booleans sequentially")
        void testMultipleBooleans() {
            ByteBuf buf = Unpooled.buffer();

            AmqpCodec.encodeBoolean(buf, true);
            AmqpCodec.encodeBoolean(buf, false);
            AmqpCodec.encodeBoolean(buf, true);

            assertThat(AmqpCodec.decodeBoolean(buf)).isTrue();
            assertThat(AmqpCodec.decodeBoolean(buf)).isFalse();
            assertThat(AmqpCodec.decodeBoolean(buf)).isTrue();
        }
    }

    @Nested
    @DisplayName("Integration Tests")
    class IntegrationTests {

        @Test
        @DisplayName("Should encode and decode complete frame roundtrip")
        void testCompleteFrameRoundtrip() {
            // Create encoder and decoder channels
            EmbeddedChannel encoderChannel = new EmbeddedChannel(new AmqpCodec.AmqpFrameEncoder());
            EmbeddedChannel decoderChannel = new EmbeddedChannel(new AmqpCodec.AmqpFrameDecoder());

            // Create original frame
            ByteBuf originalPayload = Unpooled.wrappedBuffer("test payload".getBytes());
            AmqpFrame originalFrame = new AmqpFrame((byte) 1, (short) 5, originalPayload);

            // Encode
            encoderChannel.writeOutbound(originalFrame);
            ByteBuf encoded = encoderChannel.readOutbound();

            // Decode
            decoderChannel.writeInbound(encoded);
            AmqpFrame decodedFrame = decoderChannel.readInbound();

            // Verify
            assertThat(decodedFrame.getType()).isEqualTo((byte) 1);
            assertThat(decodedFrame.getChannel()).isEqualTo((short) 5);
            assertThat(decodedFrame.getSize()).isEqualTo(12);

            byte[] decodedPayload = new byte[12];
            decodedFrame.getPayload().readBytes(decodedPayload);
            assertThat(decodedPayload).isEqualTo("test payload".getBytes());

            decodedFrame.getPayload().release();
        }

        @Test
        @DisplayName("Should handle protocol header followed by frames")
        void testProtocolHeaderThenFrames() {
            EmbeddedChannel channel = new EmbeddedChannel(
                new AmqpCodec.ProtocolHeaderDecoder(),
                new AmqpCodec.AmqpFrameEncoder()
            );

            // Send protocol header
            ByteBuf header = Unpooled.buffer();
            header.writeBytes(new byte[]{'A', 'M', 'Q', 'P', 0, 0, 9, 1});
            channel.writeInbound(header);

            // Now send a frame
            ByteBuf frameData = Unpooled.buffer();
            frameData.writeByte(1);
            frameData.writeShort(0);
            frameData.writeInt(4);
            frameData.writeBytes("test".getBytes());
            frameData.writeByte(AmqpFrame.FRAME_END);
            channel.writeInbound(frameData);

            // Should receive decoded frame
            AmqpFrame frame = channel.readInbound();
            assertThat(frame).isNotNull();
            assertThat(frame.getType()).isEqualTo((byte) 1);

            frame.getPayload().release();
        }
    }
}
