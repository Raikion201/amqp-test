package com.amqp.protocol.v10.messaging;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 Message Header section.
 *
 * Contains transport-related header information.
 *
 * Fields:
 * 0: durable (boolean) - Message is durable
 * 1: priority (ubyte) - Message priority (0-9)
 * 2: ttl (milliseconds) - Time-to-live in milliseconds
 * 3: first-acquirer (boolean) - True if this is first acquirer
 * 4: delivery-count (uint) - Number of delivery attempts
 */
public class Header implements MessageSection {

    public static final long DESCRIPTOR = AmqpType.Descriptor.HEADER;

    public static final int DEFAULT_PRIORITY = 4;

    private Boolean durable;
    private Integer priority;
    private Long ttl;
    private Boolean firstAcquirer;
    private Long deliveryCount;

    public Header() {
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public SectionType getSectionType() {
        return SectionType.HEADER;
    }

    @Override
    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(durable);
        fields.add(priority);
        fields.add(ttl);
        fields.add(firstAcquirer);
        fields.add(deliveryCount);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    // Getters
    public Boolean getDurable() {
        return durable;
    }

    public boolean isDurable() {
        return durable != null && durable;
    }

    public Integer getPriority() {
        return priority;
    }

    public int getPriorityOrDefault() {
        return priority != null ? priority : DEFAULT_PRIORITY;
    }

    public Long getTtl() {
        return ttl;
    }

    public Boolean getFirstAcquirer() {
        return firstAcquirer;
    }

    public boolean isFirstAcquirer() {
        return firstAcquirer != null && firstAcquirer;
    }

    public Long getDeliveryCount() {
        return deliveryCount;
    }

    public long getDeliveryCountOrDefault() {
        return deliveryCount != null ? deliveryCount : 0;
    }

    /**
     * Check if this message has expired based on its TTL.
     *
     * @param creationTime the message creation time in milliseconds
     * @return true if the message has expired
     */
    public boolean isExpired(long creationTime) {
        if (ttl == null || ttl == 0) {
            return false; // No TTL or TTL=0 means no expiration
        }
        return System.currentTimeMillis() > (creationTime + ttl);
    }

    /**
     * Check if this message has expired based on current time.
     * Uses now as creation time (for messages without creation time tracking).
     *
     * @param expirationTime the message expiration time in milliseconds
     * @return true if the message has expired
     */
    public static boolean isExpiredAt(long expirationTime) {
        if (expirationTime == 0) {
            return false; // No expiration
        }
        return System.currentTimeMillis() > expirationTime;
    }

    /**
     * Calculate the expiration time based on TTL.
     *
     * @param creationTime the message creation time
     * @return the expiration time, or 0 if no TTL
     */
    public long getExpirationTime(long creationTime) {
        if (ttl == null || ttl == 0) {
            return 0; // No expiration
        }
        return creationTime + ttl;
    }

    /**
     * Get remaining TTL based on creation time.
     *
     * @param creationTime the message creation time
     * @return remaining TTL in milliseconds, or 0 if expired
     */
    public long getRemainingTtl(long creationTime) {
        if (ttl == null || ttl == 0) {
            return Long.MAX_VALUE; // No TTL limit
        }
        long remaining = (creationTime + ttl) - System.currentTimeMillis();
        return Math.max(0, remaining);
    }

    // Setters
    public Header setDurable(Boolean durable) {
        this.durable = durable;
        return this;
    }

    public Header setPriority(Integer priority) {
        this.priority = priority;
        return this;
    }

    public Header setTtl(Long ttl) {
        this.ttl = ttl;
        return this;
    }

    public Header setFirstAcquirer(Boolean firstAcquirer) {
        this.firstAcquirer = firstAcquirer;
        return this;
    }

    public Header setDeliveryCount(Long deliveryCount) {
        this.deliveryCount = deliveryCount;
        return this;
    }

    public static Header decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        Header header = new Header();

        header.durable = TypeDecoder.getField(fields, 0, Boolean.class);

        Object priority = TypeDecoder.getField(fields, 1);
        if (priority instanceof Number) {
            header.priority = ((Number) priority).intValue();
        }

        Object ttl = TypeDecoder.getField(fields, 2);
        if (ttl instanceof Number) {
            header.ttl = ((Number) ttl).longValue();
        }

        header.firstAcquirer = TypeDecoder.getField(fields, 3, Boolean.class);

        Object deliveryCount = TypeDecoder.getField(fields, 4);
        if (deliveryCount instanceof Number) {
            header.deliveryCount = ((Number) deliveryCount).longValue();
        }

        return header;
    }

    @Override
    public String toString() {
        return String.format("Header{durable=%s, priority=%s, ttl=%s, firstAcquirer=%s, deliveryCount=%s}",
                durable, priority, ttl, firstAcquirer, deliveryCount);
    }
}
