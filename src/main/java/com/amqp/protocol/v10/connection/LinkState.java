package com.amqp.protocol.v10.connection;

/**
 * AMQP 1.0 Link states.
 */
public enum LinkState {
    /**
     * Link not yet attached.
     */
    DETACHED,

    /**
     * Attach sent, waiting for peer's attach.
     */
    ATTACH_SENT,

    /**
     * Attach received from peer.
     */
    ATTACH_RCVD,

    /**
     * Link is attached and active.
     */
    ATTACHED,

    /**
     * Detach sent, waiting for peer's detach.
     */
    DETACH_SENT,

    /**
     * Detach received from peer.
     */
    DETACH_RCVD,

    /**
     * Link in error state.
     */
    ERROR
}
