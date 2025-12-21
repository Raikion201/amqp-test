package com.amqp.security.sasl.amqp10;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 SASL Init frame.
 *
 * Sent by client to initiate SASL authentication.
 *
 * Fields:
 * 0: mechanism (symbol, mandatory) - Selected mechanism
 * 1: initial-response (binary) - Initial response data
 * 2: hostname (string) - Hostname of the service
 */
public class SaslInitFrame implements SaslPerformative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.SASL_INIT;

    private final Symbol mechanism;
    private byte[] initialResponse;
    private String hostname;

    public SaslInitFrame(Symbol mechanism) {
        this.mechanism = mechanism;
    }

    public SaslInitFrame(String mechanism) {
        this(Symbol.valueOf(mechanism));
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(mechanism);
        fields.add(initialResponse);
        fields.add(hostname);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    public Symbol getMechanism() {
        return mechanism;
    }

    public String getMechanismName() {
        return mechanism.toString();
    }

    public byte[] getInitialResponse() {
        return initialResponse;
    }

    public SaslInitFrame setInitialResponse(byte[] initialResponse) {
        this.initialResponse = initialResponse;
        return this;
    }

    public String getHostname() {
        return hostname;
    }

    public SaslInitFrame setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public static SaslInitFrame decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);

        Object mechObj = TypeDecoder.getField(fields, 0);
        Symbol mechanism;
        if (mechObj instanceof Symbol) {
            mechanism = (Symbol) mechObj;
        } else if (mechObj instanceof String) {
            mechanism = Symbol.valueOf((String) mechObj);
        } else {
            throw new IllegalArgumentException("Invalid mechanism");
        }

        SaslInitFrame init = new SaslInitFrame(mechanism);

        Object response = TypeDecoder.getField(fields, 1);
        if (response instanceof byte[]) {
            init.initialResponse = (byte[]) response;
        }

        init.hostname = TypeDecoder.getField(fields, 2, String.class);

        return init;
    }

    @Override
    public String toString() {
        return String.format("SaslInit{mechanism=%s, hasResponse=%s, hostname=%s}",
                mechanism, initialResponse != null, hostname);
    }
}
