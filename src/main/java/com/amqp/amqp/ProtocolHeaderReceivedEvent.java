package com.amqp.amqp;

/**
 * Event fired when the AMQP protocol header has been successfully received and validated.
 * This signals to the connection handler that it's safe to send Connection.Start.
 */
public final class ProtocolHeaderReceivedEvent {

    public static final ProtocolHeaderReceivedEvent INSTANCE = new ProtocolHeaderReceivedEvent();

    private ProtocolHeaderReceivedEvent() {
        // Singleton
    }
}
