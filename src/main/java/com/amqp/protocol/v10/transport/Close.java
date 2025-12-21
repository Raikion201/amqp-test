package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 Close performative.
 *
 * Sent to close a connection.
 *
 * Fields:
 * 0: error (error) - Error causing the close
 */
public class Close implements Performative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.CLOSE;

    private ErrorCondition error;

    public Close() {
    }

    public Close(ErrorCondition error) {
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

    public Close setError(ErrorCondition error) {
        this.error = error;
        return this;
    }

    public static Close decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        Close close = new Close();

        Object errorField = TypeDecoder.getField(fields, 0);
        if (errorField instanceof DescribedType) {
            close.error = ErrorCondition.decode((DescribedType) errorField);
        }

        return close;
    }

    @Override
    public String toString() {
        return "Close{error=" + error + "}";
    }
}
