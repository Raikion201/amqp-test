package com.amqp.security.sasl.amqp10;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 SASL Outcome frame.
 *
 * Sent by server to indicate the result of authentication.
 *
 * Fields:
 * 0: code (ubyte, mandatory) - Outcome code
 * 1: additional-data (binary) - Additional data from authentication
 */
public class SaslOutcomeFrame implements SaslPerformative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.SASL_OUTCOME;

    // Outcome codes
    public static final int OK = 0;           // Authentication successful
    public static final int AUTH = 1;         // Authentication failed
    public static final int SYS = 2;          // System error
    public static final int SYS_PERM = 3;     // Permanent system error
    public static final int SYS_TEMP = 4;     // Temporary system error

    private final int code;
    private byte[] additionalData;

    public SaslOutcomeFrame(int code) {
        this.code = code;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(code);
        fields.add(additionalData);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    public int getCode() {
        return code;
    }

    public boolean isSuccess() {
        return code == OK;
    }

    public byte[] getAdditionalData() {
        return additionalData;
    }

    public SaslOutcomeFrame setAdditionalData(byte[] additionalData) {
        this.additionalData = additionalData;
        return this;
    }

    public String getCodeDescription() {
        switch (code) {
            case OK: return "ok";
            case AUTH: return "auth";
            case SYS: return "sys";
            case SYS_PERM: return "sys-perm";
            case SYS_TEMP: return "sys-temp";
            default: return "unknown";
        }
    }

    public static SaslOutcomeFrame decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);

        int code = 0;
        Object codeObj = TypeDecoder.getField(fields, 0);
        if (codeObj instanceof Number) {
            code = ((Number) codeObj).intValue();
        }

        SaslOutcomeFrame outcome = new SaslOutcomeFrame(code);
        outcome.additionalData = TypeDecoder.getField(fields, 1, byte[].class);

        return outcome;
    }

    public static SaslOutcomeFrame success() {
        return new SaslOutcomeFrame(OK);
    }

    public static SaslOutcomeFrame authFailed() {
        return new SaslOutcomeFrame(AUTH);
    }

    public static SaslOutcomeFrame systemError() {
        return new SaslOutcomeFrame(SYS);
    }

    @Override
    public String toString() {
        return String.format("SaslOutcome{code=%d (%s)}", code, getCodeDescription());
    }
}
