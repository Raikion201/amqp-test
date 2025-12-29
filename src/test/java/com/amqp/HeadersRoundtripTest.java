package com.amqp;

import com.amqp.amqp.AmqpCodec;
import com.amqp.model.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Test to verify headers encoding/decoding roundtrip.
 */
public class HeadersRoundtripTest {

    @Test
    void testHeadersEncodeDecode() {
        // Create a message with headers
        Message original = new Message();
        Map<String, Object> headers = new HashMap<>();
        headers.put("x-custom", "value");
        headers.put("x-number", 42);
        original.setHeaders(headers);

        // Encode headers like MessageDeliveryService does
        ByteBuf headerPayload = Unpooled.buffer();

        short propertyFlags = 0;
        Map<String, Object> msgHeaders = original.getHeaders();
        if (msgHeaders != null && !msgHeaders.isEmpty()) {
            propertyFlags |= (1 << 13);
        }

        System.out.println("propertyFlags: " + Integer.toBinaryString(propertyFlags & 0xFFFF));
        System.out.println("Headers flag set: " + ((propertyFlags & (1 << 13)) != 0));

        headerPayload.writeShort(propertyFlags);

        if (msgHeaders != null && !msgHeaders.isEmpty()) {
            System.out.println("Encoding headers: " + msgHeaders);
            AmqpCodec.encodeTable(headerPayload, msgHeaders);
            System.out.println("Payload size after encoding: " + headerPayload.readableBytes());
        }

        // Now decode like AmqpChannel does
        short decodedFlags = headerPayload.readShort();
        System.out.println("Decoded flags: " + Integer.toBinaryString(decodedFlags & 0xFFFF));

        Message decoded = new Message();
        if ((decodedFlags & (1 << 13)) != 0) {
            System.out.println("Headers flag is set, decoding table...");
            Map<String, Object> decodedHeaders = AmqpCodec.decodeTable(headerPayload);
            decoded.setHeaders(decodedHeaders);
            System.out.println("Decoded headers: " + decodedHeaders);
        } else {
            System.out.println("Headers flag NOT set!");
        }

        // Verify
        assertThat(decoded.getHeaders()).isNotNull();
        assertThat(decoded.getHeaders()).hasSize(2);
        assertThat(decoded.getHeaders()).containsEntry("x-custom", "value");
        assertThat(decoded.getHeaders()).containsEntry("x-number", 42);
    }

    @Test
    void testFullContentHeaderRoundtrip() {
        // Simulate full content header encoding/decoding
        Message original = new Message();
        original.setContentType("text/plain");
        Map<String, Object> headers = new HashMap<>();
        headers.put("x-custom", "value");
        headers.put("x-number", 42);
        original.setHeaders(headers);
        original.setDeliveryMode((short) 1);

        // Encode like MessageDeliveryService (lines 318-380)
        ByteBuf headerPayload = Unpooled.buffer();

        short propertyFlags = 0;
        if (original.getContentType() != null) propertyFlags |= (1 << 15);
        if (original.getContentEncoding() != null) propertyFlags |= (1 << 14);
        if (original.getHeaders() != null && !original.getHeaders().isEmpty()) propertyFlags |= (1 << 13);
        Short deliveryMode = original.getDeliveryMode();
        if (deliveryMode != null) propertyFlags |= (1 << 12);

        System.out.println("Encoding with propertyFlags: 0x" + Integer.toHexString(propertyFlags & 0xFFFF));

        headerPayload.writeShort(propertyFlags);

        if (original.getContentType() != null) {
            AmqpCodec.encodeShortString(headerPayload, original.getContentType());
        }
        if (original.getContentEncoding() != null) {
            AmqpCodec.encodeShortString(headerPayload, original.getContentEncoding());
        }
        if (original.getHeaders() != null && !original.getHeaders().isEmpty()) {
            AmqpCodec.encodeTable(headerPayload, original.getHeaders());
        }
        if (deliveryMode != null) {
            headerPayload.writeByte(deliveryMode);
        }

        System.out.println("Encoded payload size: " + headerPayload.readableBytes());

        // Decode like AmqpChannel.handleHeaderFrame (lines 1132-1177)
        short decodedFlags = headerPayload.readShort();
        Message decoded = new Message();

        System.out.println("Decoding with propertyFlags: 0x" + Integer.toHexString(decodedFlags & 0xFFFF));

        if ((decodedFlags & (1 << 15)) != 0) {
            decoded.setContentType(AmqpCodec.decodeShortString(headerPayload));
            System.out.println("Decoded contentType: " + decoded.getContentType());
        }
        if ((decodedFlags & (1 << 14)) != 0) {
            decoded.setContentEncoding(AmqpCodec.decodeShortString(headerPayload));
        }
        if ((decodedFlags & (1 << 13)) != 0) {
            Map<String, Object> decodedHeaders = AmqpCodec.decodeTable(headerPayload);
            decoded.setHeaders(decodedHeaders);
            System.out.println("Decoded headers: " + decodedHeaders);
        }
        if ((decodedFlags & (1 << 12)) != 0) {
            decoded.setDeliveryMode(headerPayload.readByte());
            System.out.println("Decoded deliveryMode: " + decoded.getDeliveryMode());
        }

        // Verify
        assertThat(decoded.getContentType()).isEqualTo("text/plain");
        assertThat(decoded.getHeaders()).isNotNull();
        assertThat(decoded.getHeaders()).hasSize(2);
        assertThat(decoded.getHeaders()).containsEntry("x-custom", "value");
        assertThat(decoded.getHeaders()).containsEntry("x-number", 42);
        assertThat(decoded.getDeliveryMode()).isEqualTo((short) 1);
    }
}
