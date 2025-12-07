package com.amqp.amqp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import static org.assertj.core.api.Assertions.*;

@DisplayName("AMQP Method Tests")
class AmqpMethodTest {

    // Concrete implementation for testing abstract class
    private static class TestAmqpMethod extends AmqpMethod {
        public TestAmqpMethod(short classId, short methodId) {
            super(classId, methodId);
        }
    }

    @Nested
    @DisplayName("Abstract Method Class Tests")
    class AbstractMethodTests {

        @Test
        @DisplayName("Should create method with correct class and method IDs")
        void testMethodCreation() {
            AmqpMethod method = new TestAmqpMethod((short) 10, (short) 20);

            assertThat(method.getClassId()).isEqualTo((short) 10);
            assertThat(method.getMethodId()).isEqualTo((short) 20);
        }

        @Test
        @DisplayName("Should handle zero class and method IDs")
        void testMethodWithZeroIds() {
            AmqpMethod method = new TestAmqpMethod((short) 0, (short) 0);

            assertThat(method.getClassId()).isEqualTo((short) 0);
            assertThat(method.getMethodId()).isEqualTo((short) 0);
        }

        @Test
        @DisplayName("Should handle maximum short values")
        void testMethodWithMaxIds() {
            AmqpMethod method = new TestAmqpMethod(Short.MAX_VALUE, Short.MAX_VALUE);

            assertThat(method.getClassId()).isEqualTo(Short.MAX_VALUE);
            assertThat(method.getMethodId()).isEqualTo(Short.MAX_VALUE);
        }
    }

    @Nested
    @DisplayName("Class Enum Tests")
    class ClassEnumTests {

        @Test
        @DisplayName("Should have correct CONNECTION class value")
        void testConnectionClass() {
            assertThat(AmqpMethod.Class.CONNECTION.getValue()).isEqualTo((short) 10);
        }

        @Test
        @DisplayName("Should have correct CHANNEL class value")
        void testChannelClass() {
            assertThat(AmqpMethod.Class.CHANNEL.getValue()).isEqualTo((short) 20);
        }

        @Test
        @DisplayName("Should have correct EXCHANGE class value")
        void testExchangeClass() {
            assertThat(AmqpMethod.Class.EXCHANGE.getValue()).isEqualTo((short) 40);
        }

        @Test
        @DisplayName("Should have correct QUEUE class value")
        void testQueueClass() {
            assertThat(AmqpMethod.Class.QUEUE.getValue()).isEqualTo((short) 50);
        }

        @Test
        @DisplayName("Should have correct BASIC class value")
        void testBasicClass() {
            assertThat(AmqpMethod.Class.BASIC.getValue()).isEqualTo((short) 60);
        }

        @Test
        @DisplayName("Should have correct CONFIRM class value")
        void testConfirmClass() {
            assertThat(AmqpMethod.Class.CONFIRM.getValue()).isEqualTo((short) 85);
        }

        @Test
        @DisplayName("Should have all AMQP classes enumerated")
        void testAllClasses() {
            AmqpMethod.Class[] classes = AmqpMethod.Class.values();
            assertThat(classes).hasSize(6);
            assertThat(classes).contains(
                AmqpMethod.Class.CONNECTION,
                AmqpMethod.Class.CHANNEL,
                AmqpMethod.Class.EXCHANGE,
                AmqpMethod.Class.QUEUE,
                AmqpMethod.Class.BASIC,
                AmqpMethod.Class.CONFIRM
            );
        }
    }

    @Nested
    @DisplayName("ConnectionMethod Enum Tests")
    class ConnectionMethodTests {

        @Test
        @DisplayName("Should have correct START method value")
        void testStartMethod() {
            assertThat(AmqpMethod.ConnectionMethod.START.getValue()).isEqualTo((short) 10);
        }

        @Test
        @DisplayName("Should have correct START_OK method value")
        void testStartOkMethod() {
            assertThat(AmqpMethod.ConnectionMethod.START_OK.getValue()).isEqualTo((short) 11);
        }

        @Test
        @DisplayName("Should have correct SECURE method value")
        void testSecureMethod() {
            assertThat(AmqpMethod.ConnectionMethod.SECURE.getValue()).isEqualTo((short) 20);
        }

        @Test
        @DisplayName("Should have correct SECURE_OK method value")
        void testSecureOkMethod() {
            assertThat(AmqpMethod.ConnectionMethod.SECURE_OK.getValue()).isEqualTo((short) 21);
        }

        @Test
        @DisplayName("Should have correct TUNE method value")
        void testTuneMethod() {
            assertThat(AmqpMethod.ConnectionMethod.TUNE.getValue()).isEqualTo((short) 30);
        }

        @Test
        @DisplayName("Should have correct TUNE_OK method value")
        void testTuneOkMethod() {
            assertThat(AmqpMethod.ConnectionMethod.TUNE_OK.getValue()).isEqualTo((short) 31);
        }

        @Test
        @DisplayName("Should have correct OPEN method value")
        void testOpenMethod() {
            assertThat(AmqpMethod.ConnectionMethod.OPEN.getValue()).isEqualTo((short) 40);
        }

        @Test
        @DisplayName("Should have correct OPEN_OK method value")
        void testOpenOkMethod() {
            assertThat(AmqpMethod.ConnectionMethod.OPEN_OK.getValue()).isEqualTo((short) 41);
        }

        @Test
        @DisplayName("Should have correct CLOSE method value")
        void testCloseMethod() {
            assertThat(AmqpMethod.ConnectionMethod.CLOSE.getValue()).isEqualTo((short) 50);
        }

        @Test
        @DisplayName("Should have correct CLOSE_OK method value")
        void testCloseOkMethod() {
            assertThat(AmqpMethod.ConnectionMethod.CLOSE_OK.getValue()).isEqualTo((short) 51);
        }

        @Test
        @DisplayName("Should have all connection methods enumerated")
        void testAllConnectionMethods() {
            AmqpMethod.ConnectionMethod[] methods = AmqpMethod.ConnectionMethod.values();
            assertThat(methods).hasSize(10);
        }
    }

    @Nested
    @DisplayName("ChannelMethod Enum Tests")
    class ChannelMethodTests {

        @Test
        @DisplayName("Should have correct OPEN method value")
        void testOpenMethod() {
            assertThat(AmqpMethod.ChannelMethod.OPEN.getValue()).isEqualTo((short) 10);
        }

        @Test
        @DisplayName("Should have correct OPEN_OK method value")
        void testOpenOkMethod() {
            assertThat(AmqpMethod.ChannelMethod.OPEN_OK.getValue()).isEqualTo((short) 11);
        }

        @Test
        @DisplayName("Should have correct FLOW method value")
        void testFlowMethod() {
            assertThat(AmqpMethod.ChannelMethod.FLOW.getValue()).isEqualTo((short) 20);
        }

        @Test
        @DisplayName("Should have correct FLOW_OK method value")
        void testFlowOkMethod() {
            assertThat(AmqpMethod.ChannelMethod.FLOW_OK.getValue()).isEqualTo((short) 21);
        }

        @Test
        @DisplayName("Should have correct CLOSE method value")
        void testCloseMethod() {
            assertThat(AmqpMethod.ChannelMethod.CLOSE.getValue()).isEqualTo((short) 40);
        }

        @Test
        @DisplayName("Should have correct CLOSE_OK method value")
        void testCloseOkMethod() {
            assertThat(AmqpMethod.ChannelMethod.CLOSE_OK.getValue()).isEqualTo((short) 41);
        }

        @Test
        @DisplayName("Should have all channel methods enumerated")
        void testAllChannelMethods() {
            AmqpMethod.ChannelMethod[] methods = AmqpMethod.ChannelMethod.values();
            assertThat(methods).hasSize(6);
        }
    }

    @Nested
    @DisplayName("ExchangeMethod Enum Tests")
    class ExchangeMethodTests {

        @Test
        @DisplayName("Should have correct DECLARE method value")
        void testDeclareMethod() {
            assertThat(AmqpMethod.ExchangeMethod.DECLARE.getValue()).isEqualTo((short) 10);
        }

        @Test
        @DisplayName("Should have correct DECLARE_OK method value")
        void testDeclareOkMethod() {
            assertThat(AmqpMethod.ExchangeMethod.DECLARE_OK.getValue()).isEqualTo((short) 11);
        }

        @Test
        @DisplayName("Should have correct DELETE method value")
        void testDeleteMethod() {
            assertThat(AmqpMethod.ExchangeMethod.DELETE.getValue()).isEqualTo((short) 20);
        }

        @Test
        @DisplayName("Should have correct DELETE_OK method value")
        void testDeleteOkMethod() {
            assertThat(AmqpMethod.ExchangeMethod.DELETE_OK.getValue()).isEqualTo((short) 21);
        }

        @Test
        @DisplayName("Should have correct BIND method value")
        void testBindMethod() {
            assertThat(AmqpMethod.ExchangeMethod.BIND.getValue()).isEqualTo((short) 30);
        }

        @Test
        @DisplayName("Should have correct BIND_OK method value")
        void testBindOkMethod() {
            assertThat(AmqpMethod.ExchangeMethod.BIND_OK.getValue()).isEqualTo((short) 31);
        }

        @Test
        @DisplayName("Should have correct UNBIND method value")
        void testUnbindMethod() {
            assertThat(AmqpMethod.ExchangeMethod.UNBIND.getValue()).isEqualTo((short) 40);
        }

        @Test
        @DisplayName("Should have correct UNBIND_OK method value")
        void testUnbindOkMethod() {
            assertThat(AmqpMethod.ExchangeMethod.UNBIND_OK.getValue()).isEqualTo((short) 51);
        }

        @Test
        @DisplayName("Should have all exchange methods enumerated")
        void testAllExchangeMethods() {
            AmqpMethod.ExchangeMethod[] methods = AmqpMethod.ExchangeMethod.values();
            assertThat(methods).hasSize(8);
        }
    }

    @Nested
    @DisplayName("QueueMethod Enum Tests")
    class QueueMethodTests {

        @Test
        @DisplayName("Should have correct DECLARE method value")
        void testDeclareMethod() {
            assertThat(AmqpMethod.QueueMethod.DECLARE.getValue()).isEqualTo((short) 10);
        }

        @Test
        @DisplayName("Should have correct DECLARE_OK method value")
        void testDeclareOkMethod() {
            assertThat(AmqpMethod.QueueMethod.DECLARE_OK.getValue()).isEqualTo((short) 11);
        }

        @Test
        @DisplayName("Should have correct BIND method value")
        void testBindMethod() {
            assertThat(AmqpMethod.QueueMethod.BIND.getValue()).isEqualTo((short) 20);
        }

        @Test
        @DisplayName("Should have correct BIND_OK method value")
        void testBindOkMethod() {
            assertThat(AmqpMethod.QueueMethod.BIND_OK.getValue()).isEqualTo((short) 21);
        }

        @Test
        @DisplayName("Should have correct PURGE method value")
        void testPurgeMethod() {
            assertThat(AmqpMethod.QueueMethod.PURGE.getValue()).isEqualTo((short) 30);
        }

        @Test
        @DisplayName("Should have correct PURGE_OK method value")
        void testPurgeOkMethod() {
            assertThat(AmqpMethod.QueueMethod.PURGE_OK.getValue()).isEqualTo((short) 31);
        }

        @Test
        @DisplayName("Should have correct DELETE method value")
        void testDeleteMethod() {
            assertThat(AmqpMethod.QueueMethod.DELETE.getValue()).isEqualTo((short) 40);
        }

        @Test
        @DisplayName("Should have correct DELETE_OK method value")
        void testDeleteOkMethod() {
            assertThat(AmqpMethod.QueueMethod.DELETE_OK.getValue()).isEqualTo((short) 41);
        }

        @Test
        @DisplayName("Should have correct UNBIND method value")
        void testUnbindMethod() {
            assertThat(AmqpMethod.QueueMethod.UNBIND.getValue()).isEqualTo((short) 50);
        }

        @Test
        @DisplayName("Should have correct UNBIND_OK method value")
        void testUnbindOkMethod() {
            assertThat(AmqpMethod.QueueMethod.UNBIND_OK.getValue()).isEqualTo((short) 51);
        }

        @Test
        @DisplayName("Should have all queue methods enumerated")
        void testAllQueueMethods() {
            AmqpMethod.QueueMethod[] methods = AmqpMethod.QueueMethod.values();
            assertThat(methods).hasSize(10);
        }
    }

    @Nested
    @DisplayName("BasicMethod Enum Tests")
    class BasicMethodTests {

        @Test
        @DisplayName("Should have correct QOS method value")
        void testQosMethod() {
            assertThat(AmqpMethod.BasicMethod.QOS.getValue()).isEqualTo((short) 10);
        }

        @Test
        @DisplayName("Should have correct QOS_OK method value")
        void testQosOkMethod() {
            assertThat(AmqpMethod.BasicMethod.QOS_OK.getValue()).isEqualTo((short) 11);
        }

        @Test
        @DisplayName("Should have correct CONSUME method value")
        void testConsumeMethod() {
            assertThat(AmqpMethod.BasicMethod.CONSUME.getValue()).isEqualTo((short) 20);
        }

        @Test
        @DisplayName("Should have correct CONSUME_OK method value")
        void testConsumeOkMethod() {
            assertThat(AmqpMethod.BasicMethod.CONSUME_OK.getValue()).isEqualTo((short) 21);
        }

        @Test
        @DisplayName("Should have correct CANCEL method value")
        void testCancelMethod() {
            assertThat(AmqpMethod.BasicMethod.CANCEL.getValue()).isEqualTo((short) 30);
        }

        @Test
        @DisplayName("Should have correct CANCEL_OK method value")
        void testCancelOkMethod() {
            assertThat(AmqpMethod.BasicMethod.CANCEL_OK.getValue()).isEqualTo((short) 31);
        }

        @Test
        @DisplayName("Should have correct PUBLISH method value")
        void testPublishMethod() {
            assertThat(AmqpMethod.BasicMethod.PUBLISH.getValue()).isEqualTo((short) 40);
        }

        @Test
        @DisplayName("Should have correct RETURN method value")
        void testReturnMethod() {
            assertThat(AmqpMethod.BasicMethod.RETURN.getValue()).isEqualTo((short) 50);
        }

        @Test
        @DisplayName("Should have correct DELIVER method value")
        void testDeliverMethod() {
            assertThat(AmqpMethod.BasicMethod.DELIVER.getValue()).isEqualTo((short) 60);
        }

        @Test
        @DisplayName("Should have correct GET method value")
        void testGetMethod() {
            assertThat(AmqpMethod.BasicMethod.GET.getValue()).isEqualTo((short) 70);
        }

        @Test
        @DisplayName("Should have correct GET_OK method value")
        void testGetOkMethod() {
            assertThat(AmqpMethod.BasicMethod.GET_OK.getValue()).isEqualTo((short) 71);
        }

        @Test
        @DisplayName("Should have correct GET_EMPTY method value")
        void testGetEmptyMethod() {
            assertThat(AmqpMethod.BasicMethod.GET_EMPTY.getValue()).isEqualTo((short) 72);
        }

        @Test
        @DisplayName("Should have correct ACK method value")
        void testAckMethod() {
            assertThat(AmqpMethod.BasicMethod.ACK.getValue()).isEqualTo((short) 80);
        }

        @Test
        @DisplayName("Should have correct REJECT method value")
        void testRejectMethod() {
            assertThat(AmqpMethod.BasicMethod.REJECT.getValue()).isEqualTo((short) 90);
        }

        @Test
        @DisplayName("Should have correct RECOVER_ASYNC method value")
        void testRecoverAsyncMethod() {
            assertThat(AmqpMethod.BasicMethod.RECOVER_ASYNC.getValue()).isEqualTo((short) 100);
        }

        @Test
        @DisplayName("Should have correct RECOVER method value")
        void testRecoverMethod() {
            assertThat(AmqpMethod.BasicMethod.RECOVER.getValue()).isEqualTo((short) 110);
        }

        @Test
        @DisplayName("Should have correct RECOVER_OK method value")
        void testRecoverOkMethod() {
            assertThat(AmqpMethod.BasicMethod.RECOVER_OK.getValue()).isEqualTo((short) 111);
        }

        @Test
        @DisplayName("Should have correct NACK method value")
        void testNackMethod() {
            assertThat(AmqpMethod.BasicMethod.NACK.getValue()).isEqualTo((short) 120);
        }

        @Test
        @DisplayName("Should have all basic methods enumerated")
        void testAllBasicMethods() {
            AmqpMethod.BasicMethod[] methods = AmqpMethod.BasicMethod.values();
            assertThat(methods).hasSize(18);
        }
    }

    @Nested
    @DisplayName("AMQP Protocol Compliance Tests")
    class ProtocolComplianceTests {

        @Test
        @DisplayName("Should have correct class IDs according to AMQP 0-9-1 spec")
        void testClassIdsCompliance() {
            assertThat(AmqpMethod.Class.CONNECTION.getValue()).isEqualTo((short) 10);
            assertThat(AmqpMethod.Class.CHANNEL.getValue()).isEqualTo((short) 20);
            assertThat(AmqpMethod.Class.EXCHANGE.getValue()).isEqualTo((short) 40);
            assertThat(AmqpMethod.Class.QUEUE.getValue()).isEqualTo((short) 50);
            assertThat(AmqpMethod.Class.BASIC.getValue()).isEqualTo((short) 60);
        }

        @Test
        @DisplayName("Should have connection method IDs in correct sequence")
        void testConnectionMethodSequence() {
            assertThat(AmqpMethod.ConnectionMethod.START.getValue()).isLessThan(
                AmqpMethod.ConnectionMethod.START_OK.getValue());
            assertThat(AmqpMethod.ConnectionMethod.TUNE.getValue()).isLessThan(
                AmqpMethod.ConnectionMethod.TUNE_OK.getValue());
            assertThat(AmqpMethod.ConnectionMethod.OPEN.getValue()).isLessThan(
                AmqpMethod.ConnectionMethod.OPEN_OK.getValue());
        }

        @Test
        @DisplayName("Should have method pairs follow request-response pattern")
        void testMethodPairPattern() {
            // Connection methods
            assertThat(AmqpMethod.ConnectionMethod.START_OK.getValue())
                .isEqualTo((short) (AmqpMethod.ConnectionMethod.START.getValue() + 1));

            // Channel methods
            assertThat(AmqpMethod.ChannelMethod.OPEN_OK.getValue())
                .isEqualTo((short) (AmqpMethod.ChannelMethod.OPEN.getValue() + 1));

            // Queue methods
            assertThat(AmqpMethod.QueueMethod.DECLARE_OK.getValue())
                .isEqualTo((short) (AmqpMethod.QueueMethod.DECLARE.getValue() + 1));
        }
    }
}
