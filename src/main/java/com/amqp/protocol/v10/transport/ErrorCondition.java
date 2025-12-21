package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.*;

/**
 * AMQP 1.0 Error condition.
 *
 * Represents an error that occurred during processing.
 *
 * Fields:
 * 0: condition (symbol, mandatory) - Error condition identifier
 * 1: description (string) - Human-readable description
 * 2: info (map) - Additional error information
 */
public class ErrorCondition {

    public static final long DESCRIPTOR = AmqpType.Descriptor.ERROR;

    // Standard AMQP error conditions
    public static final Symbol INTERNAL_ERROR = Symbol.valueOf("amqp:internal-error");
    public static final Symbol NOT_FOUND = Symbol.valueOf("amqp:not-found");
    public static final Symbol UNAUTHORIZED_ACCESS = Symbol.valueOf("amqp:unauthorized-access");
    public static final Symbol DECODE_ERROR = Symbol.valueOf("amqp:decode-error");
    public static final Symbol RESOURCE_LIMIT_EXCEEDED = Symbol.valueOf("amqp:resource-limit-exceeded");
    public static final Symbol NOT_ALLOWED = Symbol.valueOf("amqp:not-allowed");
    public static final Symbol INVALID_FIELD = Symbol.valueOf("amqp:invalid-field");
    public static final Symbol NOT_IMPLEMENTED = Symbol.valueOf("amqp:not-implemented");
    public static final Symbol RESOURCE_LOCKED = Symbol.valueOf("amqp:resource-locked");
    public static final Symbol PRECONDITION_FAILED = Symbol.valueOf("amqp:precondition-failed");
    public static final Symbol RESOURCE_DELETED = Symbol.valueOf("amqp:resource-deleted");
    public static final Symbol ILLEGAL_STATE = Symbol.valueOf("amqp:illegal-state");
    public static final Symbol FRAME_SIZE_TOO_SMALL = Symbol.valueOf("amqp:frame-size-too-small");

    // Connection errors
    public static final Symbol CONNECTION_FORCED = Symbol.valueOf("amqp:connection:forced");
    public static final Symbol FRAMING_ERROR = Symbol.valueOf("amqp:connection:framing-error");
    public static final Symbol REDIRECT = Symbol.valueOf("amqp:connection:redirect");

    // Session errors
    public static final Symbol WINDOW_VIOLATION = Symbol.valueOf("amqp:session:window-violation");
    public static final Symbol ERRANT_LINK = Symbol.valueOf("amqp:session:errant-link");
    public static final Symbol HANDLE_IN_USE = Symbol.valueOf("amqp:session:handle-in-use");
    public static final Symbol UNATTACHED_HANDLE = Symbol.valueOf("amqp:session:unattached-handle");

    // Link errors
    public static final Symbol DETACH_FORCED = Symbol.valueOf("amqp:link:detach-forced");
    public static final Symbol TRANSFER_LIMIT_EXCEEDED = Symbol.valueOf("amqp:link:transfer-limit-exceeded");
    public static final Symbol MESSAGE_SIZE_EXCEEDED = Symbol.valueOf("amqp:link:message-size-exceeded");
    public static final Symbol LINK_REDIRECT = Symbol.valueOf("amqp:link:redirect");
    public static final Symbol STOLEN = Symbol.valueOf("amqp:link:stolen");

    private final Symbol condition;
    private String description;
    private Map<Symbol, Object> info;

    public ErrorCondition(Symbol condition) {
        this.condition = Objects.requireNonNull(condition, "condition is required");
    }

    public ErrorCondition(Symbol condition, String description) {
        this(condition);
        this.description = description;
    }

    public Symbol getCondition() {
        return condition;
    }

    public String getDescription() {
        return description;
    }

    public Map<Symbol, Object> getInfo() {
        return info;
    }

    public ErrorCondition setDescription(String description) {
        this.description = description;
        return this;
    }

    public ErrorCondition setInfo(Map<Symbol, Object> info) {
        this.info = info;
        return this;
    }

    public DescribedType toDescribed() {
        List<Object> fields = new ArrayList<>();
        fields.add(condition);
        fields.add(description);
        fields.add(info);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return new DescribedType.Default(DESCRIPTOR, fields);
    }

    @SuppressWarnings("unchecked")
    public static ErrorCondition decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);

        Object conditionObj = TypeDecoder.getField(fields, 0);
        Symbol condition;
        if (conditionObj instanceof Symbol) {
            condition = (Symbol) conditionObj;
        } else if (conditionObj instanceof String) {
            condition = Symbol.valueOf((String) conditionObj);
        } else {
            throw new IllegalArgumentException("condition is required");
        }

        ErrorCondition error = new ErrorCondition(condition);
        error.description = TypeDecoder.getField(fields, 1, String.class);

        Object infoField = TypeDecoder.getField(fields, 2);
        if (infoField instanceof Map) {
            error.info = (Map<Symbol, Object>) infoField;
        }

        return error;
    }

    @Override
    public String toString() {
        return String.format("Error{condition=%s, description='%s'}", condition, description);
    }
}
