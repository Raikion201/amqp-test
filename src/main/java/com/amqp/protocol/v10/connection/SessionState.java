package com.amqp.protocol.v10.connection;

/**
 * AMQP 1.0 Session states.
 */
public enum SessionState {
    /**
     * Session not yet begun.
     */
    UNMAPPED,

    /**
     * Begin sent, waiting for peer's begin.
     */
    BEGIN_SENT,

    /**
     * Begin received from peer.
     */
    BEGIN_RCVD,

    /**
     * Session is active.
     */
    MAPPED,

    /**
     * End sent, waiting for peer's end.
     */
    END_SENT,

    /**
     * End received from peer.
     */
    END_RCVD,

    /**
     * Session ended.
     */
    DISCARDING,

    /**
     * Session in error state.
     */
    ERROR
}
