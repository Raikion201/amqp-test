package com.amqp.amqp;

/**
 * AMQP 0-9-1 Protocol Constants.
 * Replaces magic numbers with named constants for better code maintainability.
 */
public final class AmqpConstants {

    private AmqpConstants() {
        // Utility class
    }

    // ===== Class IDs =====
    public static final short CLASS_CONNECTION = 10;
    public static final short CLASS_CHANNEL = 20;
    public static final short CLASS_EXCHANGE = 40;
    public static final short CLASS_QUEUE = 50;
    public static final short CLASS_BASIC = 60;
    public static final short CLASS_TX = 90;

    // ===== Connection Method IDs =====
    public static final short METHOD_CONNECTION_START = 10;
    public static final short METHOD_CONNECTION_START_OK = 11;
    public static final short METHOD_CONNECTION_SECURE = 20;
    public static final short METHOD_CONNECTION_SECURE_OK = 21;
    public static final short METHOD_CONNECTION_TUNE = 30;
    public static final short METHOD_CONNECTION_TUNE_OK = 31;
    public static final short METHOD_CONNECTION_OPEN = 40;
    public static final short METHOD_CONNECTION_OPEN_OK = 41;
    public static final short METHOD_CONNECTION_CLOSE = 50;
    public static final short METHOD_CONNECTION_CLOSE_OK = 51;

    // ===== Channel Method IDs =====
    public static final short METHOD_CHANNEL_OPEN = 10;
    public static final short METHOD_CHANNEL_OPEN_OK = 11;
    public static final short METHOD_CHANNEL_FLOW = 20;
    public static final short METHOD_CHANNEL_FLOW_OK = 21;
    public static final short METHOD_CHANNEL_CLOSE = 40;
    public static final short METHOD_CHANNEL_CLOSE_OK = 41;

    // ===== Exchange Method IDs =====
    public static final short METHOD_EXCHANGE_DECLARE = 10;
    public static final short METHOD_EXCHANGE_DECLARE_OK = 11;
    public static final short METHOD_EXCHANGE_DELETE = 20;
    public static final short METHOD_EXCHANGE_DELETE_OK = 21;
    public static final short METHOD_EXCHANGE_BIND = 30;
    public static final short METHOD_EXCHANGE_BIND_OK = 31;
    public static final short METHOD_EXCHANGE_UNBIND = 40;
    public static final short METHOD_EXCHANGE_UNBIND_OK = 51;

    // ===== Queue Method IDs =====
    public static final short METHOD_QUEUE_DECLARE = 10;
    public static final short METHOD_QUEUE_DECLARE_OK = 11;
    public static final short METHOD_QUEUE_BIND = 20;
    public static final short METHOD_QUEUE_BIND_OK = 21;
    public static final short METHOD_QUEUE_PURGE = 30;
    public static final short METHOD_QUEUE_PURGE_OK = 31;
    public static final short METHOD_QUEUE_DELETE = 40;
    public static final short METHOD_QUEUE_DELETE_OK = 41;
    public static final short METHOD_QUEUE_UNBIND = 50;
    public static final short METHOD_QUEUE_UNBIND_OK = 51;

    // ===== Basic Method IDs =====
    public static final short METHOD_BASIC_QOS = 10;
    public static final short METHOD_BASIC_QOS_OK = 11;
    public static final short METHOD_BASIC_CONSUME = 20;
    public static final short METHOD_BASIC_CONSUME_OK = 21;
    public static final short METHOD_BASIC_CANCEL = 30;
    public static final short METHOD_BASIC_CANCEL_OK = 31;
    public static final short METHOD_BASIC_PUBLISH = 40;
    public static final short METHOD_BASIC_RETURN = 50;
    public static final short METHOD_BASIC_DELIVER = 60;
    public static final short METHOD_BASIC_GET = 70;
    public static final short METHOD_BASIC_GET_OK = 71;
    public static final short METHOD_BASIC_GET_EMPTY = 72;
    public static final short METHOD_BASIC_ACK = 80;
    public static final short METHOD_BASIC_REJECT = 90;
    public static final short METHOD_BASIC_RECOVER_ASYNC = 100;
    public static final short METHOD_BASIC_RECOVER = 110;
    public static final short METHOD_BASIC_RECOVER_OK = 111;
    public static final short METHOD_BASIC_NACK = 120;

    // ===== TX Method IDs =====
    public static final short METHOD_TX_SELECT = 10;
    public static final short METHOD_TX_SELECT_OK = 11;
    public static final short METHOD_TX_COMMIT = 20;
    public static final short METHOD_TX_COMMIT_OK = 21;
    public static final short METHOD_TX_ROLLBACK = 30;
    public static final short METHOD_TX_ROLLBACK_OK = 31;

    // ===== AMQP Reply Codes =====
    public static final int REPLY_SUCCESS = 200;
    public static final int REPLY_CONTENT_TOO_LARGE = 311;
    public static final int REPLY_NO_ROUTE = 312;
    public static final int REPLY_NO_CONSUMERS = 313;
    public static final int REPLY_ACCESS_REFUSED = 403;
    public static final int REPLY_NOT_FOUND = 404;
    public static final int REPLY_RESOURCE_LOCKED = 405;
    public static final int REPLY_PRECONDITION_FAILED = 406;
    public static final int REPLY_CONNECTION_FORCED = 320;
    public static final int REPLY_INVALID_PATH = 402;
    public static final int REPLY_FRAME_ERROR = 501;
    public static final int REPLY_SYNTAX_ERROR = 502;
    public static final int REPLY_COMMAND_INVALID = 503;
    public static final int REPLY_CHANNEL_ERROR = 504;
    public static final int REPLY_UNEXPECTED_FRAME = 505;
    public static final int REPLY_RESOURCE_ERROR = 506;
    public static final int REPLY_NOT_ALLOWED = 530;
    public static final int REPLY_NOT_IMPLEMENTED = 540;
    public static final int REPLY_INTERNAL_ERROR = 541;

    // ===== Connection/Channel Limits =====
    public static final short DEFAULT_CHANNEL_MAX = 2047;
    public static final int DEFAULT_FRAME_MAX = 131072; // 128KB
    public static final short DEFAULT_HEARTBEAT = 60; // seconds

    // ===== Flow Control Limits =====
    public static final long FLOW_CONTROL_HIGH_WATERMARK = 400L * 1024 * 1024; // 400MB
    public static final long FLOW_CONTROL_LOW_WATERMARK = 200L * 1024 * 1024;  // 200MB
    public static final long FLOW_CONTROL_CRITICAL_WATERMARK = 50L * 1024 * 1024; // 50MB

    // ===== Message Delivery =====
    public static final int MESSAGE_DELIVERY_INTERVAL_MS = 10; // Delivery check interval

    // ===== Property Flags (for message properties) =====
    public static final short PROPERTY_FLAG_CONTENT_TYPE = (short) (1 << 15);
    public static final short PROPERTY_FLAG_CONTENT_ENCODING = (short) (1 << 14);
    public static final short PROPERTY_FLAG_HEADERS = (short) (1 << 13);
    public static final short PROPERTY_FLAG_DELIVERY_MODE = (short) (1 << 12);
    public static final short PROPERTY_FLAG_PRIORITY = (short) (1 << 11);
    public static final short PROPERTY_FLAG_CORRELATION_ID = (short) (1 << 10);
    public static final short PROPERTY_FLAG_REPLY_TO = (short) (1 << 9);
    public static final short PROPERTY_FLAG_EXPIRATION = (short) (1 << 8);
    public static final short PROPERTY_FLAG_MESSAGE_ID = (short) (1 << 7);
    public static final short PROPERTY_FLAG_TIMESTAMP = (short) (1 << 6);
    public static final short PROPERTY_FLAG_TYPE = (short) (1 << 5);
    public static final short PROPERTY_FLAG_USER_ID = (short) (1 << 4);
    public static final short PROPERTY_FLAG_APP_ID = (short) (1 << 3);
    public static final short PROPERTY_FLAG_CLUSTER_ID = (short) (1 << 2);
}
