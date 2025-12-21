package com.amqp.protocol.v10.connection;

import com.amqp.protocol.v10.delivery.DeliveryState;
import com.amqp.protocol.v10.messaging.Message10;

/**
 * Represents a delivery being tracked at the session level.
 */
public class Delivery {

    private final long deliveryId;
    private final byte[] deliveryTag;
    private final Amqp10Link link;

    private Message10 message;
    private DeliveryState state;
    private boolean settled;
    private boolean remoteSettled;

    public Delivery(long deliveryId, byte[] deliveryTag, Amqp10Link link) {
        this.deliveryId = deliveryId;
        this.deliveryTag = deliveryTag;
        this.link = link;
    }

    public long getDeliveryId() {
        return deliveryId;
    }

    public byte[] getDeliveryTag() {
        return deliveryTag;
    }

    public Amqp10Link getLink() {
        return link;
    }

    public Message10 getMessage() {
        return message;
    }

    public void setMessage(Message10 message) {
        this.message = message;
    }

    public DeliveryState getState() {
        return state;
    }

    public void setState(DeliveryState state) {
        this.state = state;
    }

    public boolean isSettled() {
        return settled;
    }

    public void setSettled(boolean settled) {
        this.settled = settled;
    }

    public boolean isRemoteSettled() {
        return remoteSettled;
    }

    public void setRemoteSettled(boolean remoteSettled) {
        this.remoteSettled = remoteSettled;
    }

    public boolean isFullySettled() {
        return settled && remoteSettled;
    }

    @Override
    public String toString() {
        return String.format("Delivery{id=%d, link=%s, settled=%s}",
                deliveryId, link.getName(), settled);
    }
}
