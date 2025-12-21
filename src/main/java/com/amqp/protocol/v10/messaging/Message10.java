package com.amqp.protocol.v10.messaging;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;
import com.amqp.protocol.v10.types.TypeEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 Message.
 *
 * A complete message consisting of sections in order:
 * 1. Header (optional, at most one)
 * 2. DeliveryAnnotations (optional, at most one)
 * 3. MessageAnnotations (optional, at most one)
 * 4. Properties (optional, at most one)
 * 5. ApplicationProperties (optional, at most one)
 * 6. Body (one or more of Data, AmqpSequence, or AmqpValue)
 * 7. Footer (optional, at most one)
 */
public class Message10 {

    private Header header;
    private DeliveryAnnotations deliveryAnnotations;
    private MessageAnnotations messageAnnotations;
    private Properties properties;
    private ApplicationProperties applicationProperties;
    private List<MessageSection> body;
    private Footer footer;

    public Message10() {
        this.body = new ArrayList<>();
    }

    // Header
    public Header getHeader() {
        return header;
    }

    public Message10 setHeader(Header header) {
        this.header = header;
        return this;
    }

    // DeliveryAnnotations
    public DeliveryAnnotations getDeliveryAnnotations() {
        return deliveryAnnotations;
    }

    public Message10 setDeliveryAnnotations(DeliveryAnnotations deliveryAnnotations) {
        this.deliveryAnnotations = deliveryAnnotations;
        return this;
    }

    // MessageAnnotations
    public MessageAnnotations getMessageAnnotations() {
        return messageAnnotations;
    }

    public Message10 setMessageAnnotations(MessageAnnotations messageAnnotations) {
        this.messageAnnotations = messageAnnotations;
        return this;
    }

    // Properties
    public Properties getProperties() {
        return properties;
    }

    public Message10 setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    // ApplicationProperties
    public ApplicationProperties getApplicationProperties() {
        return applicationProperties;
    }

    public Message10 setApplicationProperties(ApplicationProperties applicationProperties) {
        this.applicationProperties = applicationProperties;
        return this;
    }

    // Body
    public List<MessageSection> getBody() {
        return body;
    }

    public Message10 addBody(MessageSection section) {
        body.add(section);
        return this;
    }

    public Message10 setBody(Data data) {
        body.clear();
        body.add(data);
        return this;
    }

    public Message10 setBody(AmqpValue value) {
        body.clear();
        body.add(value);
        return this;
    }

    public Message10 setBody(AmqpSequence sequence) {
        body.clear();
        body.add(sequence);
        return this;
    }

    public Message10 setBody(String value) {
        return setBody(new AmqpValue(value));
    }

    public Message10 setBody(byte[] data) {
        return setBody(new Data(data));
    }

    /**
     * Get the first body section.
     */
    public MessageSection getFirstBody() {
        return body.isEmpty() ? null : body.get(0);
    }

    /**
     * Get body as string (for AmqpValue or Data bodies).
     */
    public String getBodyAsString() {
        MessageSection first = getFirstBody();
        if (first instanceof AmqpValue) {
            return ((AmqpValue) first).getValueAsString();
        } else if (first instanceof Data) {
            return ((Data) first).getValueAsString();
        }
        return null;
    }

    /**
     * Get body as bytes (for Data bodies).
     */
    public byte[] getBodyAsBytes() {
        MessageSection first = getFirstBody();
        if (first instanceof Data) {
            return ((Data) first).getValue();
        } else if (first instanceof AmqpValue) {
            Object value = ((AmqpValue) first).getValue();
            if (value instanceof byte[]) {
                return (byte[]) value;
            } else if (value instanceof String) {
                return ((String) value).getBytes();
            }
        }
        return null;
    }

    // Footer
    public Footer getFooter() {
        return footer;
    }

    public Message10 setFooter(Footer footer) {
        this.footer = footer;
        return this;
    }

    /**
     * Encode the message to a ByteBuf.
     */
    public ByteBuf encode() {
        ByteBuf buffer = Unpooled.buffer();

        if (header != null) {
            TypeEncoder.encode(header.toDescribed(), buffer);
        }
        if (deliveryAnnotations != null && !deliveryAnnotations.isEmpty()) {
            TypeEncoder.encode(deliveryAnnotations.toDescribed(), buffer);
        }
        if (messageAnnotations != null && !messageAnnotations.isEmpty()) {
            TypeEncoder.encode(messageAnnotations.toDescribed(), buffer);
        }
        if (properties != null) {
            TypeEncoder.encode(properties.toDescribed(), buffer);
        }
        if (applicationProperties != null && !applicationProperties.isEmpty()) {
            TypeEncoder.encode(applicationProperties.toDescribed(), buffer);
        }
        for (MessageSection section : body) {
            TypeEncoder.encode(section.toDescribed(), buffer);
        }
        if (footer != null && !footer.isEmpty()) {
            TypeEncoder.encode(footer.toDescribed(), buffer);
        }

        return buffer;
    }

    /**
     * Decode a message from a ByteBuf.
     */
    public static Message10 decode(ByteBuf buffer) {
        Message10 message = new Message10();

        while (buffer.isReadable()) {
            Object decoded = TypeDecoder.decode(buffer);
            if (decoded instanceof DescribedType) {
                DescribedType described = (DescribedType) decoded;
                Object descriptor = described.getDescriptor();
                long code;

                if (descriptor instanceof Number) {
                    code = ((Number) descriptor).longValue();
                } else {
                    continue;
                }

                if (code == AmqpType.Descriptor.HEADER) {
                    message.header = Header.decode(described);
                } else if (code == AmqpType.Descriptor.DELIVERY_ANNOTATIONS) {
                    message.deliveryAnnotations = DeliveryAnnotations.decode(described);
                } else if (code == AmqpType.Descriptor.MESSAGE_ANNOTATIONS) {
                    message.messageAnnotations = MessageAnnotations.decode(described);
                } else if (code == AmqpType.Descriptor.PROPERTIES) {
                    message.properties = Properties.decode(described);
                } else if (code == AmqpType.Descriptor.APPLICATION_PROPERTIES) {
                    message.applicationProperties = ApplicationProperties.decode(described);
                } else if (code == AmqpType.Descriptor.DATA) {
                    message.body.add(Data.decode(described));
                } else if (code == AmqpType.Descriptor.AMQP_VALUE) {
                    message.body.add(AmqpValue.decode(described));
                } else if (code == AmqpType.Descriptor.AMQP_SEQUENCE) {
                    message.body.add(AmqpSequence.decode(described));
                } else if (code == AmqpType.Descriptor.FOOTER) {
                    message.footer = Footer.decode(described);
                }
            }
        }

        return message;
    }

    /**
     * Create a simple text message.
     */
    public static Message10 text(String text) {
        return new Message10().setBody(new AmqpValue(text));
    }

    /**
     * Create a simple binary message.
     */
    public static Message10 binary(byte[] data) {
        return new Message10().setBody(new Data(data));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Message10{");
        if (header != null) sb.append("header=").append(header).append(", ");
        if (properties != null) sb.append("properties=").append(properties).append(", ");
        sb.append("body=").append(body);
        sb.append("}");
        return sb.toString();
    }
}
