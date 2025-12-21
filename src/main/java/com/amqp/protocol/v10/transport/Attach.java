package com.amqp.protocol.v10.transport;

import com.amqp.protocol.v10.types.AmqpType;
import com.amqp.protocol.v10.types.DescribedType;
import com.amqp.protocol.v10.types.Symbol;
import com.amqp.protocol.v10.types.TypeDecoder;

import java.util.*;

/**
 * AMQP 1.0 Attach performative.
 *
 * Sent to attach a link to a session.
 *
 * Fields:
 * 0: name (string, mandatory) - Link name
 * 1: handle (handle, mandatory) - Link handle
 * 2: role (boolean, mandatory) - Role (false=sender, true=receiver)
 * 3: snd-settle-mode (ubyte) - Sender settle mode
 * 4: rcv-settle-mode (ubyte) - Receiver settle mode
 * 5: source (source) - Source for messages
 * 6: target (target) - Target for messages
 * 7: unsettled (map) - Unsettled deliveries
 * 8: incomplete-unsettled (boolean) - Incomplete unsettled map
 * 9: initial-delivery-count (sequence-no) - Initial delivery count
 * 10: max-message-size (ulong) - Maximum message size
 * 11: offered-capabilities (symbol[]) - Offered capabilities
 * 12: desired-capabilities (symbol[]) - Desired capabilities
 * 13: properties (map) - Link properties
 */
public class Attach implements Performative {

    public static final long DESCRIPTOR = AmqpType.Descriptor.ATTACH;

    // Settle modes
    public static final int SND_SETTLE_MODE_UNSETTLED = 0;
    public static final int SND_SETTLE_MODE_SETTLED = 1;
    public static final int SND_SETTLE_MODE_MIXED = 2;

    public static final int RCV_SETTLE_MODE_FIRST = 0;
    public static final int RCV_SETTLE_MODE_SECOND = 1;

    private final String name;
    private final long handle;
    private final boolean role; // false = sender, true = receiver
    private Integer sndSettleMode;
    private Integer rcvSettleMode;
    private Source source;
    private Target target;
    private Map<Object, Object> unsettled;
    private Boolean incompleteUnsettled;
    private Long initialDeliveryCount;
    private Long maxMessageSize;
    private Symbol[] offeredCapabilities;
    private Symbol[] desiredCapabilities;
    private Map<Symbol, Object> properties;

    public Attach(String name, long handle, boolean role) {
        this.name = Objects.requireNonNull(name, "name is required");
        this.handle = handle;
        this.role = role;
    }

    @Override
    public long getDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public List<Object> getFields() {
        List<Object> fields = new ArrayList<>();
        fields.add(name);
        fields.add(handle);
        fields.add(role);
        fields.add(sndSettleMode);
        fields.add(rcvSettleMode);
        fields.add(source != null ? source.toDescribed() : null);
        fields.add(target != null ? target.toDescribed() : null);
        fields.add(unsettled);
        fields.add(incompleteUnsettled);
        fields.add(initialDeliveryCount);
        fields.add(maxMessageSize);
        fields.add(offeredCapabilities);
        fields.add(desiredCapabilities);
        fields.add(properties);

        while (!fields.isEmpty() && fields.get(fields.size() - 1) == null) {
            fields.remove(fields.size() - 1);
        }

        return fields;
    }

    // Getters
    public String getName() {
        return name;
    }

    public long getHandle() {
        return handle;
    }

    public boolean getRole() {
        return role;
    }

    public boolean isSender() {
        return !role;
    }

    public boolean isReceiver() {
        return role;
    }

    public Integer getSndSettleMode() {
        return sndSettleMode;
    }

    public Integer getRcvSettleMode() {
        return rcvSettleMode;
    }

    public Source getSource() {
        return source;
    }

    public Target getTarget() {
        return target;
    }

    public Long getInitialDeliveryCount() {
        return initialDeliveryCount;
    }

    public Long getMaxMessageSize() {
        return maxMessageSize;
    }

    // Setters
    public Attach setSndSettleMode(Integer mode) {
        this.sndSettleMode = mode;
        return this;
    }

    public Attach setRcvSettleMode(Integer mode) {
        this.rcvSettleMode = mode;
        return this;
    }

    public Attach setSource(Source source) {
        this.source = source;
        return this;
    }

    public Attach setTarget(Target target) {
        this.target = target;
        return this;
    }

    public Attach setInitialDeliveryCount(Long count) {
        this.initialDeliveryCount = count;
        return this;
    }

    public Attach setMaxMessageSize(Long size) {
        this.maxMessageSize = size;
        return this;
    }

    public Attach setOfferedCapabilities(Symbol... capabilities) {
        this.offeredCapabilities = capabilities;
        return this;
    }

    public Attach setDesiredCapabilities(Symbol... capabilities) {
        this.desiredCapabilities = capabilities;
        return this;
    }

    public Attach setProperties(Map<Symbol, Object> properties) {
        this.properties = properties;
        return this;
    }

    public static Attach decode(DescribedType described) {
        List<Object> fields = TypeDecoder.decodeFields(described);

        String name = TypeDecoder.getField(fields, 0, String.class);
        long handle = TypeDecoder.getLongField(fields, 1, 0);
        boolean role = TypeDecoder.getBooleanField(fields, 2, false);

        Attach attach = new Attach(name, handle, role);

        Object sndMode = TypeDecoder.getField(fields, 3);
        if (sndMode instanceof Number) {
            attach.sndSettleMode = ((Number) sndMode).intValue();
        }

        Object rcvMode = TypeDecoder.getField(fields, 4);
        if (rcvMode instanceof Number) {
            attach.rcvSettleMode = ((Number) rcvMode).intValue();
        }

        Object sourceField = TypeDecoder.getField(fields, 5);
        if (sourceField instanceof DescribedType) {
            attach.source = Source.decode((DescribedType) sourceField);
        }

        Object targetField = TypeDecoder.getField(fields, 6);
        if (targetField instanceof DescribedType) {
            attach.target = Target.decode((DescribedType) targetField);
        }

        Object initialCount = TypeDecoder.getField(fields, 9);
        if (initialCount instanceof Number) {
            attach.initialDeliveryCount = ((Number) initialCount).longValue();
        }

        Object maxSize = TypeDecoder.getField(fields, 10);
        if (maxSize instanceof Number) {
            attach.maxMessageSize = ((Number) maxSize).longValue();
        }

        return attach;
    }

    @Override
    public String toString() {
        return String.format("Attach{name='%s', handle=%d, role=%s, source=%s, target=%s}",
                name, handle, role ? "receiver" : "sender", source, target);
    }
}
