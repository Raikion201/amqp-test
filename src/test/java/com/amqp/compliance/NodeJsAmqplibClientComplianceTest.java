package com.amqp.compliance;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP Compliance Tests using Node.js amqplib client.
 *
 * Uses the popular Node.js AMQP client: amqplib
 * https://github.com/amqp-node/amqplib
 * https://www.npmjs.com/package/amqplib
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Node.js amqplib Client Compliance Tests")
public class NodeJsAmqplibClientComplianceTest extends DockerClientTestBase {

    private static final Logger logger = LoggerFactory.getLogger(NodeJsAmqplibClientComplianceTest.class);

    // Node.js test script using amqplib
    private static final String NODEJS_TEST_SCRIPT = """
const amqp = require('amqplib');

const AMQP_URL = process.env.AMQP_URL || 'amqp://guest:guest@amqp-server:5672/';

async function runTests() {
    console.log(`Connecting to ${AMQP_URL}`);

    try {
        // Test 1: Connection
        const connection = await amqp.connect(AMQP_URL);
        console.log('PASS: Connection established');

        // Test 2: Channel
        const channel = await connection.createChannel();
        console.log('PASS: Channel created');

        // Test 3: Queue Declaration
        const q = await channel.assertQueue('nodejs-test-queue', { autoDelete: true });
        console.log(`PASS: Queue declared: ${q.queue}`);

        // Test 4: Publish
        const message = 'Hello from Node.js amqplib!';
        channel.sendToQueue(q.queue, Buffer.from(message), {
            contentType: 'text/plain'
        });
        console.log('PASS: Message published');

        // Test 5: Consume
        await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Timeout')), 5000);
            channel.consume(q.queue, (msg) => {
                clearTimeout(timeout);
                if (msg && msg.content.toString() === message) {
                    console.log(`PASS: Message received: ${msg.content.toString()}`);
                    channel.ack(msg);
                    resolve();
                } else {
                    reject(new Error('Message mismatch'));
                }
            });
        });

        // Test 6: Exchange Declaration
        await channel.assertExchange('nodejs-test-exchange', 'direct', { autoDelete: true });
        console.log('PASS: Exchange declared');

        // Test 7: Queue Bind
        await channel.bindQueue(q.queue, 'nodejs-test-exchange', 'test-key');
        console.log('PASS: Queue bound to exchange');

        // Test 8: Publish to exchange
        channel.publish('nodejs-test-exchange', 'test-key', Buffer.from('Message via exchange'));
        console.log('PASS: Message published to exchange');

        // Wait a bit for message delivery
        await new Promise(resolve => setTimeout(resolve, 500));

        // Test 9: Get from queue
        const msg = await channel.get(q.queue, { noAck: true });
        if (msg) {
            console.log(`PASS: Message received from exchange: ${msg.content.toString()}`);
        }

        // Test 10: Prefetch/QoS
        await channel.prefetch(10);
        console.log('PASS: Prefetch set');

        // Test 11: Topic exchange
        await channel.assertExchange('nodejs-topic-exchange', 'topic', { autoDelete: true });
        console.log('PASS: Topic exchange declared');

        // Test 12: Fanout exchange
        await channel.assertExchange('nodejs-fanout-exchange', 'fanout', { autoDelete: true });
        console.log('PASS: Fanout exchange declared');

        // Test 13: Headers exchange
        await channel.assertExchange('nodejs-headers-exchange', 'headers', { autoDelete: true });
        console.log('PASS: Headers exchange declared');

        // Test 14: Confirm channel
        const confirmChannel = await connection.createConfirmChannel();
        console.log('PASS: Confirm channel created');

        // Cleanup
        await channel.deleteQueue(q.queue);
        await channel.deleteExchange('nodejs-test-exchange');
        await channel.deleteExchange('nodejs-topic-exchange');
        await channel.deleteExchange('nodejs-fanout-exchange');
        await channel.deleteExchange('nodejs-headers-exchange');

        await channel.close();
        await confirmChannel.close();
        await connection.close();

        console.log('\\n=== All Node.js amqplib tests passed! ===');

    } catch (error) {
        console.error(`FAIL: ${error.message}`);
        process.exit(1);
    }
}

runTests();
""";

    @Test
    @Order(1)
    @DisplayName("Node.js: Run amqplib compliance tests")
    void testNodeJsAmqplibClient() throws Exception {
        logger.info("Running Node.js amqplib client tests...");

        GenericContainer<?> nodeClient = new GenericContainer<>("node:20-alpine")
                .withNetwork(network)
                .withEnv("AMQP_URL", getAmqpUrl())
                .withCommand("sh", "-c",
                        "npm install --silent amqplib && " +
                        "node -e '" + NODEJS_TEST_SCRIPT.replace("'", "'\"'\"'") + "'")
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("NODEJS-CLIENT"));

        String logs = runClientAndGetLogs(nodeClient);
        logger.info("Node.js client output:\n{}", logs);

        assertTrue(logs.contains("All Node.js amqplib tests passed!"),
                "Node.js amqplib tests should pass. Output: " + logs);
    }
}
