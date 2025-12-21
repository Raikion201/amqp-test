package com.amqp.security.sasl.amqp10;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 SASL Challenge frame.
 *
 * Sent by server to challenge the client.
 *
 * Fields:
 * 0: challenge (binary, mandatory) - Challenge data
 */
public class SaslChallengeFrame implements SaslPerformative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.SASL_CHALLENGE;

    private final byte[] challenge;

    public SaslChallengeFrame(byte[] challenge) {
        this.challenge = challenge;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(challenge);
        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    public byte[] getChallenge() {
        return challenge;
    }

    public static SaslChallengeFrame decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        byte[] challenge = TypeDecoder.getField(fields, 0, byte[].class);
        return new SaslChallengeFrame(challenge);
    }

    @Override
    public String toString() {
        return String.format("SaslChallenge{challengeLength=%d}",
                challenge != null ? challenge.length : 0);
    }
}
