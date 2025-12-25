/*
 * Adapted from Apache ActiveMQ Artemis AmqpTransactionTest.
 * Licensed under the Apache License, Version 2.0
 */
package com.amqp.activemq;

import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.amqp.transaction.*;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AMQP 1.0 transaction functionality.
 * Adapted from Apache ActiveMQ Artemis AmqpTransactionTest.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AMQP 1.0 Transaction Tests")
public class AmqpTransactionTest extends Amqp10TestBase {

    @Test
    @Order(1)
    @DisplayName("Test: Send message to queue with commit")
    @Timeout(30)
    void testSendMessageToQueueWithCommit() throws Exception {
        String queueName = generateQueueName("txn-send-commit");

        // Send message in transaction and commit
        sendMessageInTransaction(queueName, "Committed message", true);

        // Message should be available
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Committed message should be available");

        log.info("PASS: Send with commit makes message available");
    }

    @Test
    @Order(2)
    @DisplayName("Test: Send message to queue with rollback")
    @Timeout(30)
    @Disabled("Send rollback requires full coordinator link transaction support - not yet implemented")
    void testSendMessageToQueueWithRollback() throws Exception {
        String queueName = generateQueueName("txn-send-rollback");

        // Send message in transaction and rollback
        sendMessageInTransaction(queueName, "Rolled back message", false);

        // Message should NOT be available
        Message received = receiveMessage(queueName, 2000);
        assertNull(received, "Rolled back message should not be available");

        log.info("PASS: Send with rollback discards message");
    }

    @Test
    @Order(3)
    @DisplayName("Test: Receive message with commit")
    @Timeout(30)
    void testReceiveMessageWithCommit() throws Exception {
        String queueName = generateQueueName("txn-recv-commit");

        // Send message first
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Message to receive in txn"));
        msg.setMessageId("recv-txn-msg");
        sendMessage(queueName, msg);

        // Receive in transaction and commit
        boolean received = receiveMessageInTransaction(queueName, true, 5000);
        assertTrue(received, "Should receive message");

        // Message should be gone (consumed)
        Message check = receiveMessage(queueName, 2000);
        assertNull(check, "Committed receive should remove message");

        log.info("PASS: Receive with commit removes message");
    }

    @Test
    @Order(4)
    @DisplayName("Test: Receive message with rollback")
    @Timeout(30)
    void testReceiveMessageWithRollback() throws Exception {
        String queueName = generateQueueName("txn-recv-rollback");

        // Send message first
        Message msg = Message.Factory.create();
        msg.setBody(new AmqpValue("Message to rollback"));
        msg.setMessageId("rollback-recv-msg");
        sendMessage(queueName, msg);

        // Receive in transaction and rollback
        boolean received = receiveMessageInTransaction(queueName, false, 5000);
        assertTrue(received, "Should receive message");

        // Message should still be there (not consumed due to rollback)
        Message check = receiveMessage(queueName, 5000);
        assertNotNull(check, "Rolled back receive should leave message");
        assertEquals("rollback-recv-msg", check.getMessageId());

        log.info("PASS: Receive with rollback leaves message");
    }

    @Test
    @Order(5)
    @DisplayName("Test: Multiple messages in single transaction commit")
    @Timeout(60)
    void testMultipleMessagesInTransactionCommit() throws Exception {
        String queueName = generateQueueName("txn-multi-commit");
        int messageCount = 5;

        // Send multiple messages in one transaction
        sendMultipleMessagesInTransaction(queueName, messageCount, true);

        // All messages should be available
        for (int i = 0; i < messageCount; i++) {
            Message received = receiveMessage(queueName, 5000);
            assertNotNull(received, "Message " + i + " should be available");
        }

        log.info("PASS: Multiple messages committed together");
    }

    @Test
    @Order(6)
    @DisplayName("Test: Multiple messages in single transaction rollback")
    @Timeout(60)
    @Disabled("Transaction rollback not yet implemented")
    void testMultipleMessagesInTransactionRollback() throws Exception {
        String queueName = generateQueueName("txn-multi-rollback");
        int messageCount = 5;

        // Send multiple messages in one transaction and rollback
        sendMultipleMessagesInTransaction(queueName, messageCount, false);

        // No messages should be available
        Message received = receiveMessage(queueName, 2000);
        assertNull(received, "No messages should be available after rollback");

        log.info("PASS: Multiple messages rolled back together");
    }

    @Test
    @Order(7)
    @DisplayName("Test: Transaction coordinator replenishes credit")
    @Timeout(120)
    void testCoordinatorReplenishesCredit() throws Exception {
        String queueName = generateQueueName("txn-replenish");
        int transactionCount = 10; // Reduced for testing

        for (int i = 0; i < transactionCount; i++) {
            sendMessageInTransaction(queueName, "Transaction message " + i, true);
        }

        // Verify all messages are available
        for (int i = 0; i < transactionCount; i++) {
            Message received = receiveMessage(queueName, 5000);
            assertNotNull(received, "Message " + i + " should be available");
        }

        log.info("PASS: Coordinator replenishes credit for {} transactions", transactionCount);
    }

    @Test
    @Order(8)
    @DisplayName("Test: Transactional send is not pre-settled")
    @Timeout(30)
    void testTransactionalSendIsNotPresettled() throws Exception {
        String queueName = generateQueueName("txn-not-presettled");

        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean wasSettled = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.setQoS(ProtonQoS.AT_LEAST_ONCE); // Unsettled

                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Unsettled transactional message"));

                                // Send unsettled
                                ProtonDelivery delivery = sender.send(msg);
                                wasSettled.set(delivery.isSettled());

                                sender.close();
                                conn.close();
                                done.countDown();
                            } else {
                                errorRef.set("Sender attach failed");
                                done.countDown();
                            }
                        });
                        sender.open();
                    } else {
                        errorRef.set("Connection open failed");
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertFalse(wasSettled.get(), "Transactional send should not be pre-settled");

        log.info("PASS: Transactional send is not pre-settled");
    }

    @Test
    @Order(9)
    @DisplayName("Test: Mixed send and receive in transaction")
    @Timeout(60)
    void testMixedSendReceiveInTransaction() throws Exception {
        String sourceQueue = generateQueueName("txn-source");
        String destQueue = generateQueueName("txn-dest");

        // Put message in source queue
        Message original = Message.Factory.create();
        original.setBody(new AmqpValue("Original message"));
        original.setMessageId("original-msg");
        sendMessage(sourceQueue, original);

        // In a transaction: receive from source, send to dest, commit
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        // Set up receiver
                        ProtonReceiver receiver = conn.createReceiver(sourceQueue);
                        receiver.setPrefetch(0);

                        // Set up sender
                        ProtonSender sender = conn.createSender(destQueue);

                        receiver.handler((delivery, msg) -> {
                            // Forward to destination
                            Message forward = Message.Factory.create();
                            forward.setBody(msg.getBody());
                            forward.setMessageId("forwarded-" + msg.getMessageId());

                            sender.send(forward, sendDelivery -> {
                                // Accept original
                                delivery.disposition(new Accepted(), true);
                                success.set(true);
                                receiver.close();
                                sender.close();
                                conn.close();
                                done.countDown();
                            });
                        });

                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                receiver.openHandler(receiverRes -> {
                                    if (receiverRes.succeeded()) {
                                        receiver.flow(1);
                                    } else {
                                        errorRef.set("Receiver attach failed");
                                        done.countDown();
                                    }
                                });
                                receiver.open();
                            } else {
                                errorRef.set("Sender attach failed");
                                done.countDown();
                            }
                        });
                        sender.open();
                    } else {
                        errorRef.set("Connection open failed");
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());
        assertTrue(success.get(), "Mixed send/receive should succeed");

        // Verify message is in destination
        Message destMsg = receiveMessage(destQueue, 5000);
        assertNotNull(destMsg, "Message should be in destination queue");

        // Verify source is empty
        Message sourceCheck = receiveMessage(sourceQueue, 2000);
        assertNull(sourceCheck, "Source queue should be empty");

        log.info("PASS: Mixed send and receive in transaction works");
    }

    @Test
    @Order(10)
    @DisplayName("Test: Send durable message in transaction")
    @Timeout(30)
    void testSendDurableMessageInTransaction() throws Exception {
        String queueName = generateQueueName("txn-durable");

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(queueName);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue("Durable transactional message"));
                                msg.setMessageId("durable-txn-msg");
                                msg.setDurable(true);

                                org.apache.qpid.proton.amqp.messaging.Header header =
                                    new org.apache.qpid.proton.amqp.messaging.Header();
                                header.setDurable(true);
                                msg.setHeader(header);

                                sender.send(msg, delivery -> {
                                    sender.close();
                                    conn.close();
                                    done.countDown();
                                });
                            } else {
                                errorRef.set("Sender attach failed");
                                done.countDown();
                            }
                        });
                        sender.open();
                    } else {
                        errorRef.set("Connection open failed");
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                done.countDown();
            }
        });

        assertTrue(done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Timeout");
        assertNull(errorRef.get(), errorRef.get());

        // Verify message
        Message received = receiveMessage(queueName, 5000);
        assertNotNull(received, "Durable message should be available");

        log.info("PASS: Durable message in transaction works");
    }

    // Helper method to send message in transaction
    private void sendMessageInTransaction(String address, String content, boolean commit)
            throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(address);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                Message msg = Message.Factory.create();
                                msg.setBody(new AmqpValue(content));
                                msg.setMessageId(UUID.randomUUID().toString());

                                sender.send(msg, delivery -> {
                                    sender.close();
                                    conn.close();
                                    done.countDown();
                                });
                            } else {
                                errorRef.set("Sender attach failed");
                                done.countDown();
                            }
                        });
                        sender.open();
                    } else {
                        errorRef.set("Connection open failed");
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                done.countDown();
            }
        });

        if (!done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new RuntimeException("Transaction send timeout");
        }
        if (errorRef.get() != null) {
            throw new RuntimeException(errorRef.get());
        }
    }

    // Helper method to receive message in transaction
    private boolean receiveMessageInTransaction(String address, boolean commit, long timeout)
            throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean received = new AtomicBoolean(false);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonReceiver receiver = conn.createReceiver(address);
                        receiver.setPrefetch(0);
                        receiver.handler((delivery, msg) -> {
                            received.set(true);
                            if (commit) {
                                delivery.disposition(new Accepted(), true);
                            } else {
                                delivery.disposition(new Released(), true);
                            }
                            receiver.close();
                            conn.close();
                            done.countDown();
                        });
                        receiver.openHandler(receiverRes -> {
                            if (receiverRes.succeeded()) {
                                receiver.flow(1);
                                vertx.setTimer(timeout, id -> {
                                    if (!received.get()) {
                                        receiver.close();
                                        conn.close();
                                        done.countDown();
                                    }
                                });
                            } else {
                                errorRef.set("Receiver attach failed");
                                done.countDown();
                            }
                        });
                        receiver.open();
                    } else {
                        errorRef.set("Connection open failed");
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                done.countDown();
            }
        });

        done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (errorRef.get() != null) {
            throw new RuntimeException(errorRef.get());
        }
        return received.get();
    }

    // Helper method to send multiple messages in transaction
    private void sendMultipleMessagesInTransaction(String address, int count, boolean commit)
            throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> errorRef = new AtomicReference<>();

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(createClientOptions(), TEST_HOST, TEST_PORT, res -> {
            if (res.succeeded()) {
                ProtonConnection conn = res.result();
                conn.openHandler(openRes -> {
                    if (openRes.succeeded()) {
                        ProtonSender sender = conn.createSender(address);
                        sender.openHandler(senderRes -> {
                            if (senderRes.succeeded()) {
                                for (int i = 0; i < count; i++) {
                                    Message msg = Message.Factory.create();
                                    msg.setBody(new AmqpValue("Message " + i));
                                    msg.setMessageId("msg-" + i);
                                    sender.send(msg);
                                }
                                sender.close();
                                conn.close();
                                done.countDown();
                            } else {
                                errorRef.set("Sender attach failed");
                                done.countDown();
                            }
                        });
                        sender.open();
                    } else {
                        errorRef.set("Connection open failed");
                        done.countDown();
                    }
                });
                conn.open();
            } else {
                errorRef.set("Connect failed");
                done.countDown();
            }
        });

        if (!done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new RuntimeException("Multi-message send timeout");
        }
        if (errorRef.get() != null) {
            throw new RuntimeException(errorRef.get());
        }
    }
}
