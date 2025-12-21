package com.amqp.protocol.v10.messaging;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * AMQP 1.0 Data body section.
 *
 * Contains binary data. A message may have multiple Data sections.
 */
public class Data implements MessageSection {

    public static final long DESCRIPTOR = AmqpType.Descriptor.DATA;

    private byte[] data;

    public Data() {
        this.data = new byte[0];
    }

    public Data(byte[] data) {
        this.data = data != null ? data : new byte[0];
    }

    public Data(String data) {
        this.data = data != null ? data.getBytes(StandardCharsets.UTF_8) : new byte[0];
    }

    public Data(ByteBuf data) {
        if (data != null && data.readableBytes() > 0) {
            this.data = new byte[data.readableBytes()];
            data.getBytes(data.readerIndex(), this.data);
        } else {
            this.data = new byte[0];
        }
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public SectionType getSectionType() {
        return SectionType.DATA;
    }

    @Override
    public DescribedType toDescribed() {
        return new DescribedType.Default(DESCRIPTOR, data);
    }

    public byte[] getValue() {
        return data;
    }

    public String getValueAsString() {
        return new String(data, StandardCharsets.UTF_8);
    }

    public ByteBuf getValueAsByteBuf() {
        return Unpooled.wrappedBuffer(data);
    }

    public int length() {
        return data.length;
    }

    public boolean isEmpty() {
        return data.length == 0;
    }

    public Data setValue(byte[] data) {
        this.data = data != null ? data : new byte[0];
        return this;
    }

    public static Data decode(DescribedType described) {
        Object value = described.getDescribed();
        if (value instanceof byte[]) {
            return new Data((byte[]) value);
        } else if (value instanceof ByteBuf) {
            return new Data((ByteBuf) value);
        } else if (value instanceof String) {
            return new Data((String) value);
        }
        return new Data();
    }

    @Override
    public String toString() {
        return String.format("Data{length=%d}", data.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Data data1 = (Data) o;
        return Arrays.equals(data, data1.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}
