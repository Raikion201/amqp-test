package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.TypeDecoder;
import com.amqp.protocol.v10.types.UByte;
import com.amqp.protocol.v10.types.UInt;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * AMQP 1.0 Transfer performative.
 *
 * Sent to transfer a message.
 *
 * Fields:
 * 0: handle (handle, mandatory) - Link handle
 * 1: delivery-id (delivery-number) - Delivery ID at sender
 * 2: delivery-tag (delivery-tag) - Delivery tag
 * 3: message-format (message-format) - Message format code
 * 4: settled (boolean) - If true, message is pre-settled
 * 5: more (boolean) - More frames for this delivery
 * 6: rcv-settle-mode (receiver-settle-mode) - Receiver settle mode override
 * 7: state (delivery-state) - State of the delivery
 * 8: resume (boolean) - Resuming a delivery
 * 9: aborted (boolean) - Aborted delivery
 * 10: batchable (boolean) - Can be batched with other transfers
 */
public class Transfer implements Performative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.TRANSFER;

    // Standard message format
    public static final long MESSAGE_FORMAT_AMQP = 0;

    private final long handle;
    private Long deliveryId;
    private byte[] deliveryTag;
    private Long messageFormat;
    private Boolean settled;
    private Boolean more;
    private Integer rcvSettleMode;
    private Object state; // DeliveryState
    private Boolean resume;
    private Boolean aborted;
    private Boolean batchable;

    // Message payload (not a field, but part of frame)
    private ByteBuf payload;

    public Transfer(long handle) {
        this.handle = handle;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public List<Object> getFields() {
        List<Object> fields = new ArrayList<>();
        // handle is uint per AMQP 1.0 spec
        fields.add(UInt.valueOf(handle));
        // delivery-id is delivery-number (uint) per AMQP 1.0 spec
        fields.add(deliveryId == null ? null : UInt.valueOf(deliveryId));
        fields.add(deliveryTag);
        // message-format is uint per AMQP 1.0 spec
        fields.add(messageFormat == null ? null : UInt.valueOf(messageFormat));
        fields.add(settled);
        fields.add(more);
        // rcv-settle-mode is ubyte per AMQP 1.0 spec
        fields.add(rcvSettleMode == null ? null : UByte.valueOf(rcvSettleMode));
        fields.add(state);
        fields.add(resume);
        fields.add(aborted);
        fields.add(batchable);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return fields;
    }

    // Getters
    public long getHandle() {
        return handle;
    }

    public Long getDeliveryId() {
        return deliveryId;
    }

    public byte[] getDeliveryTag() {
        return deliveryTag;
    }

    public Long getMessageFormat() {
        return messageFormat;
    }

    public Boolean getSettled() {
        return settled;
    }

    public boolean isSettled() {
        return settled != null && settled;
    }

    public Boolean getMore() {
        return more;
    }

    public boolean hasMore() {
        return more != null && more;
    }

    public Integer getRcvSettleMode() {
        return rcvSettleMode;
    }

    public Object getState() {
        return state;
    }

    public Boolean getResume() {
        return resume;
    }

    public boolean isResume() {
        return resume != null && resume;
    }

    public Boolean getAborted() {
        return aborted;
    }

    public boolean isAborted() {
        return aborted != null && aborted;
    }

    public Boolean getBatchable() {
        return batchable;
    }

    public boolean isBatchable() {
        return batchable != null && batchable;
    }

    public ByteBuf getPayload() {
        return payload;
    }

    // Setters
    public Transfer setDeliveryId(Long deliveryId) {
        this.deliveryId = deliveryId;
        return this;
    }

    public Transfer setDeliveryTag(byte[] deliveryTag) {
        this.deliveryTag = deliveryTag;
        return this;
    }

    public Transfer setMessageFormat(Long messageFormat) {
        this.messageFormat = messageFormat;
        return this;
    }

    public Transfer setSettled(Boolean settled) {
        this.settled = settled;
        return this;
    }

    public Transfer setMore(Boolean more) {
        this.more = more;
        return this;
    }

    public Transfer setRcvSettleMode(Integer rcvSettleMode) {
        this.rcvSettleMode = rcvSettleMode;
        return this;
    }

    public Transfer setState(Object state) {
        this.state = state;
        return this;
    }

    public Transfer setResume(Boolean resume) {
        this.resume = resume;
        return this;
    }

    public Transfer setAborted(Boolean aborted) {
        this.aborted = aborted;
        return this;
    }

    public Transfer setBatchable(Boolean batchable) {
        this.batchable = batchable;
        return this;
    }

    public Transfer setPayload(ByteBuf payload) {
        this.payload = payload;
        return this;
    }

    public static Transfer decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);

        long handle = TypeDecoder.getLongField(fields, 0, 0);
        Transfer transfer = new Transfer(handle);

        Object deliveryId = TypeDecoder.getField(fields, 1);
        if (deliveryId instanceof Number) {
            transfer.deliveryId = ((Number) deliveryId).longValue();
        }

        Object deliveryTag = TypeDecoder.getField(fields, 2);
        if (deliveryTag instanceof byte[]) {
            transfer.deliveryTag = (byte[]) deliveryTag;
        }

        Object messageFormat = TypeDecoder.getField(fields, 3);
        if (messageFormat instanceof Number) {
            transfer.messageFormat = ((Number) messageFormat).longValue();
        }

        transfer.settled = TypeDecoder.getField(fields, 4, Boolean.class);
        transfer.more = TypeDecoder.getField(fields, 5, Boolean.class);

        Object rcvSettleMode = TypeDecoder.getField(fields, 6);
        if (rcvSettleMode instanceof Number) {
            transfer.rcvSettleMode = ((Number) rcvSettleMode).intValue();
        }

        transfer.state = TypeDecoder.getField(fields, 7);
        transfer.resume = TypeDecoder.getField(fields, 8, Boolean.class);
        transfer.aborted = TypeDecoder.getField(fields, 9, Boolean.class);
        transfer.batchable = TypeDecoder.getField(fields, 10, Boolean.class);

        return transfer;
    }

    @Override
    public String toString() {
        return String.format("Transfer{handle=%d, deliveryId=%s, settled=%s, more=%s, payloadSize=%d}",
                handle, deliveryId, settled, more, payload != null ? payload.readableBytes() : 0);
    }
}
