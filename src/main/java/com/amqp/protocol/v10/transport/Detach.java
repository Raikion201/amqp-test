package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;
import com.amqp.protocol.v10.types.UInt;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 Detach performative.
 *
 * Sent to detach a link from a session.
 *
 * Fields:
 * 0: handle (handle, mandatory) - Link handle
 * 1: closed (boolean) - If true, link is closed (not just detached)
 * 2: error (error) - Error causing the detach
 */
public class Detach implements Performative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.DETACH;

    private final long handle;
    private Boolean closed;
    private ErrorCondition error;

    public Detach(long handle) {
        this.handle = handle;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public List<Object> getFields() {
        List<Object> fields = new ArrayList<>();
        // Handle must be encoded as uint (AMQP spec requirement)
        fields.add(UInt.valueOf(handle));
        fields.add(closed);
        fields.add(error != null ? error.toDescribed() : null);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return fields;
    }

    // Getters
    public long getHandle() {
        return handle;
    }

    public Boolean getClosed() {
        return closed;
    }

    public boolean isClosed() {
        return closed != null && closed;
    }

    public ErrorCondition getError() {
        return error;
    }

    // Setters
    public Detach setClosed(Boolean closed) {
        this.closed = closed;
        return this;
    }

    public Detach setError(ErrorCondition error) {
        this.error = error;
        return this;
    }

    public static Detach decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);

        long handle = TypeDecoder.getLongField(fields, 0, 0);
        Detach detach = new Detach(handle);

        detach.closed = TypeDecoder.getField(fields, 1, Boolean.class);

        Object errorField = TypeDecoder.getField(fields, 2);
        if (errorField instanceof DescribedType) {
            detach.error = ErrorCondition.decode((DescribedType) errorField);
        }

        return detach;
    }

    @Override
    public String toString() {
        return String.format("Detach{handle=%d, closed=%s, error=%s}",
                handle, closed, error);
    }
}
