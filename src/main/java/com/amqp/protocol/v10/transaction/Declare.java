package com.amqp.protocol.v10.transaction;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 Declare message.
 *
 * Message body for declaring a transaction. The declare message is sent
 * to the coordinator to request a new transaction-id. A successful
 * declare results in a Declared outcome being returned.
 *
 * Fields:
 * 0: global-id (binary) - Optional global transaction id for distributed transactions
 */
public class Declare {

    public static final long DESCRIPTOR = AmqpType.Descriptor.DECLARE;

    private byte[] globalId;

    public Declare() {
    }

    public Declare(byte[] globalId) {
        this.globalId = globalId;
    }

    public byte[] getGlobalId() {
        return globalId;
    }

    public Declare setGlobalId(byte[] globalId) {
        this.globalId = globalId;
        return this;
    }

    public boolean hasGlobalId() {
        return globalId != null && globalId.length > 0;
    }

    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(globalId);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    public static Declare decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        Declare declare = new Declare();

        Object globalIdField = TypeDecoder.getField(fields, 0);
        if (globalIdField instanceof byte[]) {
            declare.globalId = (byte[]) globalIdField;
        }

        return declare;
    }

    @Override
    public String toString() {
        if (globalId != null) {
            StringBuilder sb = new StringBuilder("Declare{globalId=");
            for (byte b : globalId) {
                sb.append(String.format("%02x", b));
            }
            sb.append("}");
            return sb.toString();
        }
        return "Declare{}";
    }
}
