package com.amqp.protocol.v10.delivery;

import com.amqp.protocol.v10.transport.ErrorCondition;
import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 Rejected delivery state.
 *
 * Indicates the message was rejected and should not be redelivered.
 * The message can be considered "dead-lettered".
 *
 * Fields:
 * 0: error (error) - Error explaining the rejection
 */
public class Rejected implements DeliveryState {

    public static final long DESCRIPTOR = AmqpType.Descriptor.REJECTED;

    private ErrorCondition error;

    public Rejected() {
    }

    public Rejected(ErrorCondition error) {
        this.error = error;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(error != null ? error.toDescribed() : null);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    @Override
    public boolean isTerminal() {
        return true;
    }

    public ErrorCondition getError() {
        return error;
    }

    public Rejected setError(ErrorCondition error) {
        this.error = error;
        return this;
    }

    public static Rejected decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        Rejected rejected = new Rejected();

        Object errorField = TypeDecoder.getField(fields, 0);
        if (errorField instanceof DescribedType) {
            rejected.error = ErrorCondition.decode((DescribedType) errorField);
        }

        return rejected;
    }

    @Override
    public String toString() {
        return "Rejected{error=" + error + "}";
    }
}
