package com.amqp.security.sasl.amqp10;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * AMQP 1.0 SASL Mechanisms frame.
 *
 * Sent by server to advertise available SASL mechanisms.
 *
 * Fields:
 * 0: sasl-server-mechanisms (symbol[]) - Available mechanisms
 */
public class SaslMechanismsFrame implements SaslPerformative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.SASL_MECHANISMS;

    private final Symbol[] mechanisms;

    public SaslMechanismsFrame(Symbol... mechanisms) {
        this.mechanisms = mechanisms;
    }

    public SaslMechanismsFrame(List<String> mechanisms) {
        this.mechanisms = mechanisms.stream()
                .map(Symbol::valueOf)
                .toArray(Symbol[]::new);
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(mechanisms);
        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    public Symbol[] getMechanisms() {
        return mechanisms;
    }

    public List<String> getMechanismNames() {
        List<String> names = new ArrayList<>();
        for (Symbol s : mechanisms) {
            names.add(s.toString());
        }
        return names;
    }

    public boolean hasMechanism(String name) {
        for (Symbol s : mechanisms) {
            if (s.toString().equals(name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "SaslMechanisms{mechanisms=" + Arrays.toString(mechanisms) + "}";
    }
}
