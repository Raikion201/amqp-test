package com.amqp.security.sasl.amqp10;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 SASL Response frame.
 *
 * Sent by client in response to a challenge.
 *
 * Fields:
 * 0: response (binary, mandatory) - Response data
 */
public class SaslResponseFrame implements SaslPerformative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.SASL_RESPONSE;

    private final byte[] response;

    public SaslResponseFrame(byte[] response) {
        this.response = response;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(response);
        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    public byte[] getResponse() {
        return response;
    }

    public static SaslResponseFrame decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        byte[] response = TypeDecoder.getField(fields, 0, byte[].class);
        return new SaslResponseFrame(response);
    }

    @Override
    public String toString() {
        return String.format("SaslResponse{responseLength=%d}",
                response != null ? response.length : 0);
    }
}
