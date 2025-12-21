package com.amqp.protocol.v10.connection;

import com.amqp.protocol.v10.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP 1.0 Link base class.
 *
 * Links are unidirectional and come in two types:
 * - Sender: sends messages to the remote peer
 * - Receiver: receives messages from the remote peer
 */
public abstract class Amqp10Link {

    private static final Logger log = LoggerFactory.getLogger(Amqp10Link.class);

    protected final Amqp10Session session;
    protected final String name;
    protected final long handle;
    protected final boolean role; // false = sender, true = receiver

    protected Source source;
    protected Target target;

    protected volatile LinkState state = LinkState.DETACHED;

    // Settle modes
    protected Integer sndSettleMode;
    protected Integer rcvSettleMode;

    // Flow control
    protected long deliveryCount = 0;
    protected long linkCredit = 0;
    protected long available = 0;
    protected boolean drain = false;

    // Error tracking
    protected ErrorCondition error;

    protected Amqp10Link(Amqp10Session session, String name, long handle, boolean role,
                         Source source, Target target) {
        this.session = session;
        this.name = name;
        this.handle = handle;
        this.role = role;
        this.source = source;
        this.target = target;
    }

    // State management
    public LinkState getState() {
        return state;
    }

    public void setState(LinkState state) {
        log.debug("Link {} state change: {} -> {}", name, this.state, state);
        this.state = state;
    }

    public boolean isAttached() {
        return state == LinkState.ATTACHED;
    }

    public boolean isDetaching() {
        return state == LinkState.DETACH_SENT || state == LinkState.DETACH_RCVD;
    }

    // Attach handling
    public void onAttachReceived(Attach attach) {
        // Update source/target from peer
        if (attach.getSource() != null) {
            this.source = attach.getSource();
        }
        if (attach.getTarget() != null) {
            this.target = attach.getTarget();
        }

        // Negotiate settle modes
        if (attach.getSndSettleMode() != null) {
            this.sndSettleMode = attach.getSndSettleMode();
        }
        if (attach.getRcvSettleMode() != null) {
            this.rcvSettleMode = attach.getRcvSettleMode();
        }

        // For senders, get initial delivery count from attach
        if (isSender() && attach.getInitialDeliveryCount() != null) {
            // This is from the remote receiver
        } else if (isReceiver() && attach.getInitialDeliveryCount() != null) {
            // We're a receiver, sender's initial count
            this.deliveryCount = attach.getInitialDeliveryCount();
        }

        switch (state) {
            case DETACHED:
                setState(LinkState.ATTACH_RCVD);
                break;
            case ATTACH_SENT:
                setState(LinkState.ATTACHED);
                break;
            default:
                log.warn("Unexpected Attach in state {}", state);
        }
    }

    public void onAttachSent() {
        switch (state) {
            case DETACHED:
                setState(LinkState.ATTACH_SENT);
                break;
            case ATTACH_RCVD:
                setState(LinkState.ATTACHED);
                break;
            default:
                log.warn("Unexpected Attach sent in state {}", state);
        }
    }

    // Detach handling
    public void onDetachReceived(Detach detach) {
        if (detach.getError() != null) {
            this.error = detach.getError();
            log.warn("Link {} detached by peer with error: {}", name, error);
        }

        switch (state) {
            case ATTACHED:
                setState(LinkState.DETACH_RCVD);
                break;
            case DETACH_SENT:
                setState(LinkState.DETACHED);
                session.removeLink(handle);
                break;
            default:
                setState(LinkState.DETACHED);
                session.removeLink(handle);
        }
    }

    public void onDetachSent() {
        switch (state) {
            case ATTACHED:
                setState(LinkState.DETACH_SENT);
                break;
            case DETACH_RCVD:
                setState(LinkState.DETACHED);
                session.removeLink(handle);
                break;
            default:
                setState(LinkState.DETACHED);
        }
    }

    // Flow handling
    public void onFlowReceived(Flow flow) {
        if (flow.getDeliveryCount() != null) {
            this.deliveryCount = flow.getDeliveryCount();
        }
        if (flow.getLinkCredit() != null) {
            this.linkCredit = flow.getLinkCredit();
        }
        if (flow.getAvailable() != null) {
            this.available = flow.getAvailable();
        }
        if (flow.getDrain() != null) {
            this.drain = flow.getDrain();
        }
    }

    // Create performatives
    public Attach createAttach() {
        Attach attach = new Attach(name, handle, role);
        attach.setSource(source);
        attach.setTarget(target);
        if (sndSettleMode != null) {
            attach.setSndSettleMode(sndSettleMode);
        }
        if (rcvSettleMode != null) {
            attach.setRcvSettleMode(rcvSettleMode);
        }
        if (isSender()) {
            attach.setInitialDeliveryCount(deliveryCount);
        }
        return attach;
    }

    public Detach createDetach(boolean closed) {
        Detach detach = new Detach(handle);
        detach.setClosed(closed);
        if (error != null) {
            detach.setError(error);
        }
        return detach;
    }

    public Detach createDetach(boolean closed, ErrorCondition error) {
        this.error = error;
        Detach detach = new Detach(handle);
        detach.setClosed(closed);
        detach.setError(error);
        return detach;
    }

    public Flow createFlow() {
        Flow flow = session.createFlow();
        flow.setHandle(handle);
        flow.setDeliveryCount(deliveryCount);
        flow.setLinkCredit(linkCredit);
        flow.setAvailable(available);
        flow.setDrain(drain);
        return flow;
    }

    public void close(ErrorCondition error) {
        if (state != LinkState.DETACHED && state != LinkState.DETACH_SENT) {
            this.error = error;
            // The handler should send Detach
        }
    }

    // Role helpers
    public boolean isSender() {
        return !role;
    }

    public boolean isReceiver() {
        return role;
    }

    // Credit operations
    public boolean hasCredit() {
        return linkCredit > 0;
    }

    public void consumeCredit() {
        if (linkCredit > 0) {
            linkCredit--;
        }
    }

    public void addCredit(long credit) {
        this.linkCredit += credit;
    }

    // Getters
    public Amqp10Session getSession() {
        return session;
    }

    public String getName() {
        return name;
    }

    public long getHandle() {
        return handle;
    }

    public boolean getRole() {
        return role;
    }

    public Source getSource() {
        return source;
    }

    public Target getTarget() {
        return target;
    }

    public Integer getSndSettleMode() {
        return sndSettleMode;
    }

    public Integer getRcvSettleMode() {
        return rcvSettleMode;
    }

    public long getDeliveryCount() {
        return deliveryCount;
    }

    public long getLinkCredit() {
        return linkCredit;
    }

    public long getAvailable() {
        return available;
    }

    public boolean isDrain() {
        return drain;
    }

    public ErrorCondition getError() {
        return error;
    }

    // Setters
    public void setSource(Source source) {
        this.source = source;
    }

    public void setTarget(Target target) {
        this.target = target;
    }

    public void setSndSettleMode(Integer mode) {
        this.sndSettleMode = mode;
    }

    public void setRcvSettleMode(Integer mode) {
        this.rcvSettleMode = mode;
    }

    public void setLinkCredit(long credit) {
        this.linkCredit = credit;
    }

    public void setAvailable(long available) {
        this.available = available;
    }

    public void setDrain(boolean drain) {
        this.drain = drain;
    }

    @Override
    public String toString() {
        return String.format("Amqp10Link{name='%s', handle=%d, role=%s, state=%s}",
                name, handle, role ? "receiver" : "sender", state);
    }
}
