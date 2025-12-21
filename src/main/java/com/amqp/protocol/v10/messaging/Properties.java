package com.amqp.protocol.v10.messaging;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * AMQP 1.0 Message Properties section.
 *
 * Contains immutable message properties.
 *
 * Fields:
 * 0: message-id (message-id) - Application message identifier
 * 1: user-id (binary) - Creating user identity
 * 2: to (address) - Destination address
 * 3: subject (string) - Message subject
 * 4: reply-to (address) - Reply-to address
 * 5: correlation-id (message-id) - Correlation identifier
 * 6: content-type (symbol) - MIME content type
 * 7: content-encoding (symbol) - MIME content encoding
 * 8: absolute-expiry-time (timestamp) - Absolute expiry time
 * 9: creation-time (timestamp) - Creation time
 * 10: group-id (string) - Group identifier
 * 11: group-sequence (sequence-no) - Sequence number within group
 * 12: reply-to-group-id (string) - Reply-to group identifier
 */
public class Properties implements MessageSection {

    public static final long DESCRIPTOR = AmqpType.Descriptor.PROPERTIES;

    private Object messageId; // String, UUID, binary, or ulong
    private byte[] userId;
    private String to;
    private String subject;
    private String replyTo;
    private Object correlationId; // String, UUID, binary, or ulong
    private Symbol contentType;
    private Symbol contentEncoding;
    private Date absoluteExpiryTime;
    private Date creationTime;
    private String groupId;
    private Long groupSequence;
    private String replyToGroupId;

    public Properties() {
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public SectionType getSectionType() {
        return SectionType.PROPERTIES;
    }

    @Override
    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(messageId);
        fields.add(userId);
        fields.add(to);
        fields.add(subject);
        fields.add(replyTo);
        fields.add(correlationId);
        fields.add(contentType);
        fields.add(contentEncoding);
        fields.add(absoluteExpiryTime);
        fields.add(creationTime);
        fields.add(groupId);
        fields.add(groupSequence);
        fields.add(replyToGroupId);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    // Getters
    public Object getMessageId() {
        return messageId;
    }

    public String getMessageIdAsString() {
        if (messageId instanceof String) {
            return (String) messageId;
        } else if (messageId != null) {
            return messageId.toString();
        }
        return null;
    }

    public byte[] getUserId() {
        return userId;
    }

    public String getTo() {
        return to;
    }

    public String getSubject() {
        return subject;
    }

    public String getReplyTo() {
        return replyTo;
    }

    public Object getCorrelationId() {
        return correlationId;
    }

    public String getCorrelationIdAsString() {
        if (correlationId instanceof String) {
            return (String) correlationId;
        } else if (correlationId != null) {
            return correlationId.toString();
        }
        return null;
    }

    public Symbol getContentType() {
        return contentType;
    }

    public String getContentTypeAsString() {
        return contentType != null ? contentType.toString() : null;
    }

    public Symbol getContentEncoding() {
        return contentEncoding;
    }

    public Date getAbsoluteExpiryTime() {
        return absoluteExpiryTime;
    }

    public Date getCreationTime() {
        return creationTime;
    }

    public String getGroupId() {
        return groupId;
    }

    public Long getGroupSequence() {
        return groupSequence;
    }

    public String getReplyToGroupId() {
        return replyToGroupId;
    }

    // Setters
    public Properties setMessageId(Object messageId) {
        this.messageId = messageId;
        return this;
    }

    public Properties setMessageId(String messageId) {
        this.messageId = messageId;
        return this;
    }

    public Properties setMessageId(UUID messageId) {
        this.messageId = messageId;
        return this;
    }

    public Properties setUserId(byte[] userId) {
        this.userId = userId;
        return this;
    }

    public Properties setTo(String to) {
        this.to = to;
        return this;
    }

    public Properties setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public Properties setReplyTo(String replyTo) {
        this.replyTo = replyTo;
        return this;
    }

    public Properties setCorrelationId(Object correlationId) {
        this.correlationId = correlationId;
        return this;
    }

    public Properties setContentType(Symbol contentType) {
        this.contentType = contentType;
        return this;
    }

    public Properties setContentType(String contentType) {
        this.contentType = Symbol.valueOf(contentType);
        return this;
    }

    public Properties setContentEncoding(Symbol contentEncoding) {
        this.contentEncoding = contentEncoding;
        return this;
    }

    public Properties setAbsoluteExpiryTime(Date absoluteExpiryTime) {
        this.absoluteExpiryTime = absoluteExpiryTime;
        return this;
    }

    public Properties setCreationTime(Date creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    public Properties setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public Properties setGroupSequence(Long groupSequence) {
        this.groupSequence = groupSequence;
        return this;
    }

    public Properties setReplyToGroupId(String replyToGroupId) {
        this.replyToGroupId = replyToGroupId;
        return this;
    }

    public static Properties decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        Properties props = new Properties();

        props.messageId = TypeDecoder.getField(fields, 0);
        props.userId = TypeDecoder.getField(fields, 1, byte[].class);
        props.to = TypeDecoder.getField(fields, 2, String.class);
        props.subject = TypeDecoder.getField(fields, 3, String.class);
        props.replyTo = TypeDecoder.getField(fields, 4, String.class);
        props.correlationId = TypeDecoder.getField(fields, 5);

        Object contentType = TypeDecoder.getField(fields, 6);
        if (contentType instanceof Symbol) {
            props.contentType = (Symbol) contentType;
        } else if (contentType instanceof String) {
            props.contentType = Symbol.valueOf((String) contentType);
        }

        Object contentEncoding = TypeDecoder.getField(fields, 7);
        if (contentEncoding instanceof Symbol) {
            props.contentEncoding = (Symbol) contentEncoding;
        } else if (contentEncoding instanceof String) {
            props.contentEncoding = Symbol.valueOf((String) contentEncoding);
        }

        Object expiryTime = TypeDecoder.getField(fields, 8);
        if (expiryTime instanceof Date) {
            props.absoluteExpiryTime = (Date) expiryTime;
        } else if (expiryTime instanceof Number) {
            props.absoluteExpiryTime = new Date(((Number) expiryTime).longValue());
        }

        Object creationTime = TypeDecoder.getField(fields, 9);
        if (creationTime instanceof Date) {
            props.creationTime = (Date) creationTime;
        } else if (creationTime instanceof Number) {
            props.creationTime = new Date(((Number) creationTime).longValue());
        }

        props.groupId = TypeDecoder.getField(fields, 10, String.class);

        Object groupSequence = TypeDecoder.getField(fields, 11);
        if (groupSequence instanceof Number) {
            props.groupSequence = ((Number) groupSequence).longValue();
        }

        props.replyToGroupId = TypeDecoder.getField(fields, 12, String.class);

        return props;
    }

    @Override
    public String toString() {
        return String.format("Properties{messageId=%s, to='%s', subject='%s', contentType=%s}",
                messageId, to, subject, contentType);
    }
}
