/*
 * Adapted from Apache ActiveMQ Artemis AMQP protocol tests.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.protocol.v10.messaging;

import com.amqp.protocol.v10.types.DescribedType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 Message Header section.
 * Adapted from Apache ActiveMQ Artemis HeaderTest.
 */
@DisplayName("AMQP 1.0 Message Header Tests")
public class HeaderTest {

    @Test
    @DisplayName("Header: Should have correct descriptor")
    void testDescriptor() {
        Header header = new Header();
        assertEquals(0x70L, header.getDescriptor());
    }

    @Test
    @DisplayName("Header: Should have correct section type")
    void testSectionType() {
        Header header = new Header();
        assertEquals(MessageSection.SectionType.HEADER, header.getSectionType());
    }

    @Test
    @DisplayName("Header: Default values should be null")
    void testDefaultValues() {
        Header header = new Header();
        assertNull(header.getDurable());
        assertNull(header.getPriority());
        assertNull(header.getTtl());
        assertNull(header.getFirstAcquirer());
        assertNull(header.getDeliveryCount());
    }

    @Test
    @DisplayName("Header: isDurable should return false when null")
    void testIsDurableWhenNull() {
        Header header = new Header();
        assertFalse(header.isDurable());
    }

    @Test
    @DisplayName("Header: isDurable should return true when set")
    void testIsDurableWhenTrue() {
        Header header = new Header().setDurable(true);
        assertTrue(header.isDurable());
    }

    @Test
    @DisplayName("Header: isDurable should return false when explicitly false")
    void testIsDurableWhenFalse() {
        Header header = new Header().setDurable(false);
        assertFalse(header.isDurable());
    }

    @Test
    @DisplayName("Header: getPriorityOrDefault returns default when null")
    void testPriorityOrDefault() {
        Header header = new Header();
        assertEquals(Header.DEFAULT_PRIORITY, header.getPriorityOrDefault());
    }

    @Test
    @DisplayName("Header: getPriorityOrDefault returns set value")
    void testPriorityOrDefaultWithValue() {
        Header header = new Header().setPriority(9);
        assertEquals(9, header.getPriorityOrDefault());
    }

    @Test
    @DisplayName("Header: Should set durable")
    void testSetDurable() {
        Header header = new Header().setDurable(true);
        assertEquals(Boolean.TRUE, header.getDurable());
    }

    @Test
    @DisplayName("Header: Should set priority")
    void testSetPriority() {
        Header header = new Header().setPriority(7);
        assertEquals(Integer.valueOf(7), header.getPriority());
    }

    @Test
    @DisplayName("Header: Should set TTL")
    void testSetTtl() {
        Header header = new Header().setTtl(60000L);
        assertEquals(Long.valueOf(60000L), header.getTtl());
    }

    @Test
    @DisplayName("Header: Should set first acquirer")
    void testSetFirstAcquirer() {
        Header header = new Header().setFirstAcquirer(true);
        assertEquals(Boolean.TRUE, header.getFirstAcquirer());
        assertTrue(header.isFirstAcquirer());
    }

    @Test
    @DisplayName("Header: isFirstAcquirer returns false when null")
    void testIsFirstAcquirerWhenNull() {
        Header header = new Header();
        assertFalse(header.isFirstAcquirer());
    }

    @Test
    @DisplayName("Header: Should set delivery count")
    void testSetDeliveryCount() {
        Header header = new Header().setDeliveryCount(5L);
        assertEquals(Long.valueOf(5L), header.getDeliveryCount());
    }

    @Test
    @DisplayName("Header: getDeliveryCountOrDefault returns 0 when null")
    void testDeliveryCountOrDefault() {
        Header header = new Header();
        assertEquals(0L, header.getDeliveryCountOrDefault());
    }

    @Test
    @DisplayName("Header: getDeliveryCountOrDefault returns set value")
    void testDeliveryCountOrDefaultWithValue() {
        Header header = new Header().setDeliveryCount(3L);
        assertEquals(3L, header.getDeliveryCountOrDefault());
    }

    @Test
    @DisplayName("Header: Builder pattern should be chainable")
    void testBuilderChaining() {
        Header header = new Header()
                .setDurable(true)
                .setPriority(8)
                .setTtl(30000L)
                .setFirstAcquirer(true)
                .setDeliveryCount(2L);

        assertTrue(header.isDurable());
        assertEquals(8, header.getPriorityOrDefault());
        assertEquals(30000L, header.getTtl().longValue());
        assertTrue(header.isFirstAcquirer());
        assertEquals(2L, header.getDeliveryCountOrDefault());
    }

    @Test
    @DisplayName("Header: toDescribed should produce valid DescribedType")
    void testToDescribed() {
        Header header = new Header()
                .setDurable(true)
                .setPriority(5);

        DescribedType described = header.toDescribed();

        assertNotNull(described);
        assertEquals(Header.DESCRIPTOR, described.getDescriptor());
    }

    @Test
    @DisplayName("Header: toDescribed should trim trailing nulls")
    void testToDescribedTrimsNulls() {
        Header header = new Header().setDurable(true);
        DescribedType described = header.toDescribed();

        Object value = described.getDescribed();
        if (value instanceof java.util.List) {
            java.util.List<?> list = (java.util.List<?>) value;
            assertEquals(1, list.size());
        }
    }

    @Test
    @DisplayName("Header: toString should include all fields")
    void testToString() {
        Header header = new Header()
                .setDurable(true)
                .setPriority(7)
                .setTtl(60000L);

        String str = header.toString();

        assertTrue(str.contains("durable=true"));
        assertTrue(str.contains("priority=7"));
        assertTrue(str.contains("ttl=60000"));
    }

    @Test
    @DisplayName("Header: Priority bounds check - minimum")
    void testPriorityMinimum() {
        Header header = new Header().setPriority(0);
        assertEquals(0, header.getPriority().intValue());
    }

    @Test
    @DisplayName("Header: Priority bounds check - maximum")
    void testPriorityMaximum() {
        Header header = new Header().setPriority(9);
        assertEquals(9, header.getPriority().intValue());
    }

    @Test
    @DisplayName("Header: TTL of zero means no expiration")
    void testTtlZero() {
        Header header = new Header().setTtl(0L);
        assertEquals(0L, header.getTtl().longValue());
    }

    @Test
    @DisplayName("Header: Large TTL value")
    void testLargeTtl() {
        long largeTtl = 365L * 24 * 60 * 60 * 1000; // 1 year in milliseconds
        Header header = new Header().setTtl(largeTtl);
        assertEquals(largeTtl, header.getTtl().longValue());
    }
}
