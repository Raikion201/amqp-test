package com.amqp.protocol.v10.transaction;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 Coordinator terminus.
 *
 * The coordinator terminus is used to initiate transaction processing.
 * A sending link to a coordinator can be used to declare transactions
 * and to associate and/or discharge them.
 *
 * Fields:
 * 0: capabilities (symbol[]) - Coordinator capabilities
 */
public class Coordinator {

    public static final long DESCRIPTOR = AmqpType.Descriptor.COORDINATOR;

    // Transaction capability symbols
    public static final Symbol TXN_LOCAL = Symbol.valueOf("amqp:local-transactions");
    public static final Symbol TXN_DISTRIBUTED = Symbol.valueOf("amqp:distributed-transactions");
    public static final Symbol TXN_PROMOTABLE = Symbol.valueOf("amqp:promotable-transactions");
    public static final Symbol TXN_MULTI_TXNS = Symbol.valueOf("amqp:multi-txns-per-ssn");
    public static final Symbol TXN_MULTI_SSNS = Symbol.valueOf("amqp:multi-ssns-per-txn");

    private Symbol[] capabilities;

    public Coordinator() {
    }

    public Coordinator(Symbol... capabilities) {
        this.capabilities = capabilities;
    }

    public Symbol[] getCapabilities() {
        return capabilities;
    }

    public Coordinator setCapabilities(Symbol... capabilities) {
        this.capabilities = capabilities;
        return this;
    }

    public boolean hasCapability(Symbol capability) {
        if (capabilities == null) {
            return false;
        }
        for (Symbol cap : capabilities) {
            if (cap.equals(capability)) {
                return true;
            }
        }
        return false;
    }

    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(capabilities);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    public static Coordinator decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);
        Coordinator coordinator = new Coordinator();

        Object capsField = TypeDecoder.getField(fields, 0);
        if (capsField instanceof Symbol[]) {
            coordinator.capabilities = (Symbol[]) capsField;
        } else if (capsField instanceof Object[]) {
            Object[] arr = (Object[]) capsField;
            coordinator.capabilities = new Symbol[arr.length];
            for (int i = 0; i < arr.length; i++) {
                if (arr[i] instanceof Symbol) {
                    coordinator.capabilities[i] = (Symbol) arr[i];
                } else if (arr[i] instanceof String) {
                    coordinator.capabilities[i] = Symbol.valueOf((String) arr[i]);
                }
            }
        }

        return coordinator;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Coordinator{capabilities=[");
        if (capabilities != null) {
            for (int i = 0; i < capabilities.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(capabilities[i]);
            }
        }
        sb.append("]}");
        return sb.toString();
    }
}
