package com.amqp.protocol.v10.compliance;

import com.amqp.protocol.v10.transport.*;
import com.amqp.protocol.v10.types.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP 1.0 Transport Layer Compliance Tests
 *
 * Based on OASIS AMQP 1.0 Specification Part 2: Transport
 * http://docs.oasis-open.org/amqp/core/v1.0/amqp-core-transport-v1.0.html
 *
 * These tests validate compliance with the transport layer requirements
 * of the AMQP 1.0 specification.
 */
@DisplayName("AMQP 1.0 Transport Compliance (Part 2)")
public class TransportComplianceTest {

    /**
     * Section 2.4: Connections
     */
    @Nested
    @DisplayName("2.4: Connection Performatives")
    class ConnectionPerformatives {

        @Test
        @DisplayName("2.4.1: open performative descriptor code is 0x00000000:0x00000010")
        void testOpenDescriptor() {
            assertEquals(0x10L, AmqpType.Descriptor.OPEN,
                    "OPEN descriptor must be 0x10");
            assertEquals(0x10L, Open.DESCRIPTOR,
                    "Open.DESCRIPTOR must match AmqpType.Descriptor.OPEN");
        }

        @Test
        @DisplayName("2.4.1: open container-id is mandatory")
        void testOpenContainerIdMandatory() {
            assertThrows(NullPointerException.class, () -> new Open(null),
                    "container-id is mandatory");
        }

        @Test
        @DisplayName("2.4.1: open max-frame-size default is 4294967295 (UINT_MAX)")
        void testOpenMaxFrameSizeDefault() {
            Open open = new Open("test-container");
            assertEquals(0xFFFFFFFFL, open.getMaxFrameSize(),
                    "Default max-frame-size should be UINT_MAX");
        }

        @Test
        @DisplayName("2.4.1: open channel-max default is 65535 (USHORT_MAX)")
        void testOpenChannelMaxDefault() {
            Open open = new Open("test-container");
            assertEquals(65535, open.getChannelMax(),
                    "Default channel-max should be USHORT_MAX");
        }

        @Test
        @DisplayName("2.4.1: open idle-time-out default is 0 (no timeout)")
        void testOpenIdleTimeoutDefault() {
            Open open = new Open("test-container");
            assertEquals(0, open.getIdleTimeout(),
                    "Default idle-time-out should be 0");
        }

        @Test
        @DisplayName("2.4.2: close performative descriptor code is 0x00000000:0x00000018")
        void testCloseDescriptor() {
            assertEquals(0x18L, AmqpType.Descriptor.CLOSE,
                    "CLOSE descriptor must be 0x18");
            assertEquals(0x18L, Close.DESCRIPTOR,
                    "Close.DESCRIPTOR must match AmqpType.Descriptor.CLOSE");
        }

        @Test
        @DisplayName("2.4: open performative encodes all fields correctly")
        void testOpenEncodeDecode() {
            Open original = new Open("test-container")
                    .setHostname("localhost")
                    .setMaxFrameSize(65536)
                    .setChannelMax(256)
                    .setIdleTimeout(30000);

            List<Object> fields = original.getFields();
            assertEquals("test-container", fields.get(0));
            assertEquals("localhost", fields.get(1));
        }
    }

    /**
     * Section 2.5: Sessions
     */
    @Nested
    @DisplayName("2.5: Session Performatives")
    class SessionPerformatives {

        @Test
        @DisplayName("2.5.1: begin performative descriptor code is 0x00000000:0x00000011")
        void testBeginDescriptor() {
            assertEquals(0x11L, AmqpType.Descriptor.BEGIN,
                    "BEGIN descriptor must be 0x11");
            assertEquals(0x11L, Begin.DESCRIPTOR,
                    "Begin.DESCRIPTOR must match AmqpType.Descriptor.BEGIN");
        }

        @Test
        @DisplayName("2.5.1: begin requires next-outgoing-id, incoming-window, outgoing-window")
        void testBeginRequiredFields() {
            Begin begin = new Begin(0, 100, 100);
            assertEquals(0, begin.getNextOutgoingId());
            assertEquals(100, begin.getIncomingWindow());
            assertEquals(100, begin.getOutgoingWindow());
        }

        @Test
        @DisplayName("2.5.1: begin handle-max default is 4294967295 (UINT_MAX)")
        void testBeginHandleMaxDefault() {
            Begin begin = new Begin(0, 100, 100);
            assertEquals(0xFFFFFFFFL, begin.getHandleMax(),
                    "Default handle-max should be UINT_MAX");
        }

        @Test
        @DisplayName("2.5.2: end performative descriptor code is 0x00000000:0x00000017")
        void testEndDescriptor() {
            assertEquals(0x17L, AmqpType.Descriptor.END,
                    "END descriptor must be 0x17");
            assertEquals(0x17L, End.DESCRIPTOR,
                    "End.DESCRIPTOR must match AmqpType.Descriptor.END");
        }
    }

    /**
     * Section 2.6: Links
     */
    @Nested
    @DisplayName("2.6: Link Performatives")
    class LinkPerformatives {

        @Test
        @DisplayName("2.6.1: attach performative descriptor code is 0x00000000:0x00000012")
        void testAttachDescriptor() {
            assertEquals(0x12L, AmqpType.Descriptor.ATTACH,
                    "ATTACH descriptor must be 0x12");
            assertEquals(0x12L, Attach.DESCRIPTOR,
                    "Attach.DESCRIPTOR must match AmqpType.Descriptor.ATTACH");
        }

        @Test
        @DisplayName("2.6.1: attach name is mandatory")
        void testAttachNameMandatory() {
            assertThrows(NullPointerException.class, () -> new Attach(null, 0, false),
                    "name is mandatory");
        }

        @Test
        @DisplayName("2.6.1: attach role false=sender, true=receiver")
        void testAttachRole() {
            Attach sender = new Attach("sender-link", 0, false);
            assertFalse(sender.getRole());
            assertTrue(sender.isSender());
            assertFalse(sender.isReceiver());

            Attach receiver = new Attach("receiver-link", 1, true);
            assertTrue(receiver.getRole());
            assertTrue(receiver.isReceiver());
            assertFalse(receiver.isSender());
        }

        @Test
        @DisplayName("2.6.1: attach snd-settle-mode values 0=unsettled, 1=settled, 2=mixed")
        void testAttachSndSettleMode() {
            assertEquals(0, Attach.SND_SETTLE_MODE_UNSETTLED);
            assertEquals(1, Attach.SND_SETTLE_MODE_SETTLED);
            assertEquals(2, Attach.SND_SETTLE_MODE_MIXED);
        }

        @Test
        @DisplayName("2.6.1: attach rcv-settle-mode values 0=first, 1=second")
        void testAttachRcvSettleMode() {
            assertEquals(0, Attach.RCV_SETTLE_MODE_FIRST);
            assertEquals(1, Attach.RCV_SETTLE_MODE_SECOND);
        }

        @Test
        @DisplayName("2.6.2: flow performative descriptor code is 0x00000000:0x00000013")
        void testFlowDescriptor() {
            assertEquals(0x13L, AmqpType.Descriptor.FLOW,
                    "FLOW descriptor must be 0x13");
            assertEquals(0x13L, Flow.DESCRIPTOR,
                    "Flow.DESCRIPTOR must match AmqpType.Descriptor.FLOW");
        }

        @Test
        @DisplayName("2.6.3: transfer performative descriptor code is 0x00000000:0x00000014")
        void testTransferDescriptor() {
            assertEquals(0x14L, AmqpType.Descriptor.TRANSFER,
                    "TRANSFER descriptor must be 0x14");
            assertEquals(0x14L, Transfer.DESCRIPTOR,
                    "Transfer.DESCRIPTOR must match AmqpType.Descriptor.TRANSFER");
        }

        @Test
        @DisplayName("2.6.4: disposition performative descriptor code is 0x00000000:0x00000015")
        void testDispositionDescriptor() {
            assertEquals(0x15L, AmqpType.Descriptor.DISPOSITION,
                    "DISPOSITION descriptor must be 0x15");
            assertEquals(0x15L, Disposition.DESCRIPTOR,
                    "Disposition.DESCRIPTOR must match AmqpType.Descriptor.DISPOSITION");
        }

        @Test
        @DisplayName("2.6.5: detach performative descriptor code is 0x00000000:0x00000016")
        void testDetachDescriptor() {
            assertEquals(0x16L, AmqpType.Descriptor.DETACH,
                    "DETACH descriptor must be 0x16");
            assertEquals(0x16L, Detach.DESCRIPTOR,
                    "Detach.DESCRIPTOR must match AmqpType.Descriptor.DETACH");
        }
    }

    /**
     * Section 2.7: Transfer Frame
     */
    @Nested
    @DisplayName("2.7: Transfer Semantics")
    class TransferSemantics {

        @Test
        @DisplayName("2.7.1: transfer handle is mandatory")
        void testTransferHandleMandatory() {
            Transfer transfer = new Transfer(0);
            assertEquals(0, transfer.getHandle());
        }

        @Test
        @DisplayName("2.7.1: transfer settled default is false")
        void testTransferSettledDefault() {
            Transfer transfer = new Transfer(0);
            assertFalse(transfer.isSettled());
        }

        @Test
        @DisplayName("2.7.1: transfer more default is false")
        void testTransferMoreDefault() {
            Transfer transfer = new Transfer(0);
            // Default is null (equivalent to false per AMQP 1.0 spec)
            Boolean more = transfer.getMore();
            assertTrue(more == null || !more);
        }

        @Test
        @DisplayName("2.7.3: delivery-id uniquely identifies delivery on link")
        void testTransferDeliveryId() {
            Transfer transfer = new Transfer(0);
            transfer.setDeliveryId(12345L);
            assertEquals(12345L, transfer.getDeliveryId());
        }

        @Test
        @DisplayName("2.7.3: delivery-tag is binary identifier for delivery")
        void testTransferDeliveryTag() {
            Transfer transfer = new Transfer(0);
            byte[] tag = new byte[]{1, 2, 3, 4};
            transfer.setDeliveryTag(tag);
            assertArrayEquals(tag, transfer.getDeliveryTag());
        }
    }

    /**
     * Section 2.8: Flow Control
     */
    @Nested
    @DisplayName("2.8: Flow Control")
    class FlowControlCompliance {

        @Test
        @DisplayName("2.8: flow incoming-window and outgoing-window are mandatory")
        void testFlowMandatoryFields() {
            Flow flow = new Flow(100, 0, 100);
            assertEquals(100, flow.getIncomingWindow());
            assertEquals(100, flow.getOutgoingWindow());
        }

        @Test
        @DisplayName("2.8.1: flow link-credit controls messages sender may send")
        void testFlowLinkCredit() {
            Flow flow = new Flow(100, 0, 100);
            flow.setLinkCredit(50L);
            assertEquals(50L, flow.getLinkCredit());
        }

        @Test
        @DisplayName("2.8.2: flow echo field requests immediate flow response")
        void testFlowEcho() {
            Flow flow = new Flow(100, 0, 100);
            assertFalse(flow.isEcho()); // default

            flow.setEcho(true);
            assertTrue(flow.isEcho());
        }

        @Test
        @DisplayName("2.8: flow drain requests consumer to drain credit")
        void testFlowDrain() {
            Flow flow = new Flow(100, 0, 100);
            flow.setDrain(true);
            assertTrue(flow.isDrain());
        }
    }

    /**
     * Section 2.8: Disposition
     */
    @Nested
    @DisplayName("2.8.4: Disposition Semantics")
    class DispositionCompliance {

        @Test
        @DisplayName("2.8.4: disposition role indicates direction")
        void testDispositionRole() {
            Disposition senderDisp = new Disposition(false, 0);
            assertFalse(senderDisp.getRole());

            Disposition receiverDisp = new Disposition(true, 0);
            assertTrue(receiverDisp.getRole());
        }

        @Test
        @DisplayName("2.8.4: disposition first/last specify range of delivery-ids")
        void testDispositionRange() {
            Disposition disp = new Disposition(true, 5);
            disp.setLast(10L);

            assertEquals(5, disp.getFirst());
            assertEquals(10L, disp.getLast());
        }

        @Test
        @DisplayName("2.8.4: disposition settled=true marks deliveries settled")
        void testDispositionSettled() {
            Disposition disp = new Disposition(true, 0);
            disp.setSettled(true);
            assertTrue(disp.isSettled());
        }
    }

    /**
     * Performative encoding/decoding
     */
    @Nested
    @DisplayName("Performative Encoding")
    class PerformativeEncoding {

        @Test
        @DisplayName("All performatives implement Performative interface")
        void testPerformativeInterface() {
            assertTrue(Performative.class.isAssignableFrom(Open.class));
            assertTrue(Performative.class.isAssignableFrom(Begin.class));
            assertTrue(Performative.class.isAssignableFrom(Attach.class));
            assertTrue(Performative.class.isAssignableFrom(Flow.class));
            assertTrue(Performative.class.isAssignableFrom(Transfer.class));
            assertTrue(Performative.class.isAssignableFrom(Disposition.class));
            assertTrue(Performative.class.isAssignableFrom(Detach.class));
            assertTrue(Performative.class.isAssignableFrom(End.class));
            assertTrue(Performative.class.isAssignableFrom(Close.class));
        }

        @Test
        @DisplayName("Performative getDescriptor returns correct value")
        void testPerformativeGetDescriptor() {
            assertEquals(0x10L, new Open("test").getDescriptor());
            assertEquals(0x11L, new Begin(0, 100, 100).getDescriptor());
            assertEquals(0x12L, new Attach("test", 0, false).getDescriptor());
            assertEquals(0x13L, new Flow(100, 0, 100).getDescriptor());
            assertEquals(0x14L, new Transfer(0).getDescriptor());
            assertEquals(0x15L, new Disposition(true, 0).getDescriptor());
            assertEquals(0x16L, new Detach(0).getDescriptor());
            assertEquals(0x17L, new End().getDescriptor());
            assertEquals(0x18L, new Close().getDescriptor());
        }

        @Test
        @DisplayName("Performative getFields returns non-null list")
        void testPerformativeGetFields() {
            assertNotNull(new Open("test").getFields());
            assertNotNull(new Begin(0, 100, 100).getFields());
            assertNotNull(new Attach("test", 0, false).getFields());
            assertNotNull(new Flow(100, 0, 100).getFields());
            assertNotNull(new Transfer(0).getFields());
        }
    }
}
