package com.amqp.protocol.v10.connection;

/**
 * AMQP 1.0 Connection states.
 *
 * The connection lifecycle follows these states:
 * START -> HDR-RCVD/HDR-SENT -> HDR-EXCH -> OPEN-PIPE/OPEN-RCVD/OPEN-SENT ->
 * OPENED -> CLOSE-PIPE/CLOSE-RCVD/CLOSE-SENT -> END
 */
public enum ConnectionState {
    /**
     * Initial state, no communication yet.
     */
    START,

    /**
     * Header received from peer.
     */
    HDR_RCVD,

    /**
     * Header sent to peer.
     */
    HDR_SENT,

    /**
     * Headers exchanged, can begin OPEN.
     */
    HDR_EXCH,

    /**
     * Open sent before receiving peer's header.
     */
    OPEN_PIPE,

    /**
     * Open received from peer.
     */
    OPEN_RCVD,

    /**
     * Open sent to peer.
     */
    OPEN_SENT,

    /**
     * Connection fully opened.
     */
    OPENED,

    /**
     * Close sent before receiving peer's open.
     */
    CLOSE_PIPE,

    /**
     * Close received from peer.
     */
    CLOSE_RCVD,

    /**
     * Close sent to peer.
     */
    CLOSE_SENT,

    /**
     * Connection ended.
     */
    END,

    /**
     * Connection in error state.
     */
    ERROR
}
