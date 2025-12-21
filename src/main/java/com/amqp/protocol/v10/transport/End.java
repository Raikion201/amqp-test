package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 End performative.
 *
 * Sent to end a session.
 *
 * Fields:
 * 0: error (error) - Error causing the end
 */
public class End implements Performative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.END;

    private ErrorCondition error;

    public End() {
    }

    public End(ErrorCondition error) {
        this.error = error;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public List<Object> getFields() {
        List<Object> fields = new ArrayList<>();
        fields.add(error != null ? error.toDescribed() : null);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return fields;
    }

    public ErrorCondition getError() {
        return error;
    }

    public End setError(ErrorCondition error) {
        this.error = error;
        return this;
    }

    public static End decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        End end = new End();

        Object errorField = TypeDecoder.getField(fields, 0);
        if (errorField instanceof DescribedType) {
            end.error = ErrorCondition.decode((DescribedType) errorField);
        }

        return end;
    }

    @Override
    public String toString() {
        return "End{error=" + error + "}";
    }
}
