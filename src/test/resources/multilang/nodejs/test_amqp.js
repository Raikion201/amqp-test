#!/usr/bin/env node
/**
 * AMQP 0-9-1 Compliance Test using Node.js amqplib library.
 * Tests basic AMQP operations against the server.
 */

const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

class AmqpComplianceTest {
    constructor(host = 'localhost', port = 5672) {
        this.host = host;
        this.port = port;
        this.results = [];
        this.connection = null;
        this.channel = null;
    }

    async connect() {
        const url = `amqp://guest:guest@${this.host}:${this.port}`;
        this.connection = await amqp.connect(url);
        this.channel = await this.connection.createChannel();
    }

    async disconnect() {
        if (this.channel) await this.channel.close().catch(() => {});
        if (this.connection) await this.connection.close().catch(() => {});
    }

    async runTest(name, testFunc) {
        try {
            await testFunc();
            this.results.push({ test: name, status: 'PASS', error: null });
            console.log(`  [PASS] ${name}`);
            return true;
        } catch (e) {
            this.results.push({ test: name, status: 'FAIL', error: e.message });
            console.log(`  [FAIL] ${name}: ${e.message}`);
            return false;
        }
    }

    // ==================== Connection Tests ====================

    async testConnectionOpen() {
        if (!this.connection) throw new Error('Connection should be open');
    }

    async testChannelOpen() {
        if (!this.channel) throw new Error('Channel should be open');
    }

    // ==================== Queue Tests ====================

    async testQueueDeclare() {
        const queueName = `test.nodejs.queue.${uuidv4().substring(0, 8)}`;
        const result = await this.channel.assertQueue(queueName, { autoDelete: true });
        if (result.queue !== queueName) throw new Error('Queue name mismatch');
    }

    async testQueueDeclareAnonymous() {
        const result = await this.channel.assertQueue('', { exclusive: true });
        if (!result.queue || result.queue === '') throw new Error('Anonymous queue should have a name');
    }

    // ==================== Exchange Tests ====================

    async testExchangeDeclareDirect() {
        const exchangeName = `test.nodejs.direct.${uuidv4().substring(0, 8)}`;
        await this.channel.assertExchange(exchangeName, 'direct', { autoDelete: true });
    }

    async testExchangeDeclareFanout() {
        const exchangeName = `test.nodejs.fanout.${uuidv4().substring(0, 8)}`;
        await this.channel.assertExchange(exchangeName, 'fanout', { autoDelete: true });
    }

    async testExchangeDeclareTopic() {
        const exchangeName = `test.nodejs.topic.${uuidv4().substring(0, 8)}`;
        await this.channel.assertExchange(exchangeName, 'topic', { autoDelete: true });
    }

    // ==================== Binding Tests ====================

    async testQueueBind() {
        const exchangeName = `test.nodejs.bind.ex.${uuidv4().substring(0, 8)}`;
        const queueName = `test.nodejs.bind.q.${uuidv4().substring(0, 8)}`;

        await this.channel.assertExchange(exchangeName, 'direct', { autoDelete: true });
        await this.channel.assertQueue(queueName, { autoDelete: true });
        await this.channel.bindQueue(queueName, exchangeName, 'test.key');
    }

    // ==================== Publish/Consume Tests ====================

    async testBasicPublishConsume() {
        const queueName = `test.nodejs.pubsub.${uuidv4().substring(0, 8)}`;
        const testMessage = `Hello from Node.js! ${uuidv4()}`;

        await this.channel.assertQueue(queueName, { autoDelete: true });
        this.channel.sendToQueue(queueName, Buffer.from(testMessage));

        const received = await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Timeout waiting for message')), 5000);
            this.channel.consume(queueName, (msg) => {
                if (msg) {
                    clearTimeout(timeout);
                    this.channel.ack(msg);
                    resolve(msg.content.toString());
                }
            });
        });

        if (received !== testMessage) throw new Error(`Message mismatch: ${received} !== ${testMessage}`);
    }

    async testPublishWithProperties() {
        const queueName = `test.nodejs.props.${uuidv4().substring(0, 8)}`;

        await this.channel.assertQueue(queueName, { autoDelete: true });
        this.channel.sendToQueue(queueName, Buffer.from('test'), {
            contentType: 'text/plain',
            correlationId: 'corr-123',
            messageId: 'msg-456',
            priority: 5
        });

        const msg = await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Timeout waiting for message')), 5000);
            this.channel.consume(queueName, (m) => {
                if (m) {
                    clearTimeout(timeout);
                    this.channel.ack(m);
                    resolve(m);
                }
            });
        });

        if (msg.properties.contentType !== 'text/plain') throw new Error('contentType mismatch');
        if (msg.properties.correlationId !== 'corr-123') throw new Error('correlationId mismatch');
        if (msg.properties.messageId !== 'msg-456') throw new Error('messageId mismatch');
    }

    async testPublishWithHeaders() {
        const queueName = `test.nodejs.headers.${uuidv4().substring(0, 8)}`;

        await this.channel.assertQueue(queueName, { autoDelete: true });
        this.channel.sendToQueue(queueName, Buffer.from('test'), {
            headers: {
                'x-custom-header': 'custom-value',
                'x-number': 42
            }
        });

        const msg = await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Timeout waiting for message')), 5000);
            this.channel.consume(queueName, (m) => {
                if (m) {
                    clearTimeout(timeout);
                    this.channel.ack(m);
                    resolve(m);
                }
            });
        });

        if (!msg.properties.headers) throw new Error('Headers should exist');
        if (msg.properties.headers['x-custom-header'] !== 'custom-value') {
            throw new Error(`Header mismatch: ${msg.properties.headers['x-custom-header']}`);
        }
    }

    async testMessageOrdering() {
        const queueName = `test.nodejs.order.${uuidv4().substring(0, 8)}`;
        const messageCount = 10;

        await this.channel.assertQueue(queueName, { autoDelete: true });

        for (let i = 0; i < messageCount; i++) {
            this.channel.sendToQueue(queueName, Buffer.from(`Message-${i}`));
        }

        const received = [];
        await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Timeout waiting for messages')), 10000);
            this.channel.consume(queueName, (msg) => {
                if (msg) {
                    received.push(msg.content.toString());
                    this.channel.ack(msg);
                    if (received.length === messageCount) {
                        clearTimeout(timeout);
                        resolve();
                    }
                }
            });
        });

        for (let i = 0; i < messageCount; i++) {
            if (received[i] !== `Message-${i}`) {
                throw new Error(`Out of order at ${i}: ${received[i]}`);
            }
        }
    }

    // ==================== Acknowledgment Tests ====================

    async testBasicNackRequeue() {
        const queueName = `test.nodejs.nack.${uuidv4().substring(0, 8)}`;
        let deliveryCount = 0;

        await this.channel.assertQueue(queueName, { autoDelete: true });
        this.channel.sendToQueue(queueName, Buffer.from('nack-me'));

        await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Timeout')), 5000);
            this.channel.consume(queueName, (msg) => {
                if (msg) {
                    deliveryCount++;
                    if (deliveryCount === 1) {
                        this.channel.nack(msg, false, true); // requeue
                    } else {
                        this.channel.ack(msg);
                        clearTimeout(timeout);
                        resolve();
                    }
                }
            });
        });

        if (deliveryCount !== 2) throw new Error(`Expected 2 deliveries, got ${deliveryCount}`);
    }

    // ==================== QoS Tests ====================

    async testBasicQos() {
        await this.channel.prefetch(5);
        const queueName = `test.nodejs.qos.${uuidv4().substring(0, 8)}`;
        await this.channel.assertQueue(queueName, { autoDelete: true });

        for (let i = 0; i < 10; i++) {
            this.channel.sendToQueue(queueName, Buffer.from(`Message-${i}`));
        }
    }

    // ==================== Exchange Routing Tests ====================

    async testDirectRouting() {
        const exchangeName = `test.nodejs.routing.direct.${uuidv4().substring(0, 8)}`;
        const queue1 = `test.nodejs.routing.q1.${uuidv4().substring(0, 8)}`;
        const queue2 = `test.nodejs.routing.q2.${uuidv4().substring(0, 8)}`;

        await this.channel.assertExchange(exchangeName, 'direct', { autoDelete: true });
        await this.channel.assertQueue(queue1, { autoDelete: true });
        await this.channel.assertQueue(queue2, { autoDelete: true });
        await this.channel.bindQueue(queue1, exchangeName, 'key1');
        await this.channel.bindQueue(queue2, exchangeName, 'key2');

        this.channel.publish(exchangeName, 'key1', Buffer.from('to-q1'));
        this.channel.publish(exchangeName, 'key2', Buffer.from('to-q2'));

        const receivedQ1 = [];
        const receivedQ2 = [];

        await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Timeout')), 5000);
            let count = 0;

            this.channel.consume(queue1, (msg) => {
                if (msg) {
                    receivedQ1.push(msg.content.toString());
                    this.channel.ack(msg);
                    if (++count === 2) { clearTimeout(timeout); resolve(); }
                }
            });

            this.channel.consume(queue2, (msg) => {
                if (msg) {
                    receivedQ2.push(msg.content.toString());
                    this.channel.ack(msg);
                    if (++count === 2) { clearTimeout(timeout); resolve(); }
                }
            });
        });

        if (receivedQ1[0] !== 'to-q1') throw new Error('Direct routing to q1 failed');
        if (receivedQ2[0] !== 'to-q2') throw new Error('Direct routing to q2 failed');
    }

    async testFanoutRouting() {
        const exchangeName = `test.nodejs.routing.fanout.${uuidv4().substring(0, 8)}`;
        const queue1 = `test.nodejs.fanout.q1.${uuidv4().substring(0, 8)}`;
        const queue2 = `test.nodejs.fanout.q2.${uuidv4().substring(0, 8)}`;

        await this.channel.assertExchange(exchangeName, 'fanout', { autoDelete: true });
        await this.channel.assertQueue(queue1, { autoDelete: true });
        await this.channel.assertQueue(queue2, { autoDelete: true });
        await this.channel.bindQueue(queue1, exchangeName, '');
        await this.channel.bindQueue(queue2, exchangeName, '');

        this.channel.publish(exchangeName, '', Buffer.from('broadcast'));

        const receivedQ1 = [];
        const receivedQ2 = [];

        await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Timeout')), 5000);
            let count = 0;

            this.channel.consume(queue1, (msg) => {
                if (msg) {
                    receivedQ1.push(msg.content.toString());
                    this.channel.ack(msg);
                    if (++count === 2) { clearTimeout(timeout); resolve(); }
                }
            });

            this.channel.consume(queue2, (msg) => {
                if (msg) {
                    receivedQ2.push(msg.content.toString());
                    this.channel.ack(msg);
                    if (++count === 2) { clearTimeout(timeout); resolve(); }
                }
            });
        });

        if (receivedQ1[0] !== 'broadcast') throw new Error('Fanout to q1 failed');
        if (receivedQ2[0] !== 'broadcast') throw new Error('Fanout to q2 failed');
    }

    async runAllTests() {
        console.log('\n' + '='.repeat(60));
        console.log('Node.js amqplib AMQP 0-9-1 Compliance Tests');
        console.log(`Server: ${this.host}:${this.port}`);
        console.log('='.repeat(60) + '\n');

        const tests = [
            ['connection_open', () => this.testConnectionOpen()],
            ['channel_open', () => this.testChannelOpen()],
            ['queue_declare', () => this.testQueueDeclare()],
            ['queue_declare_anonymous', () => this.testQueueDeclareAnonymous()],
            ['exchange_declare_direct', () => this.testExchangeDeclareDirect()],
            ['exchange_declare_fanout', () => this.testExchangeDeclareFanout()],
            ['exchange_declare_topic', () => this.testExchangeDeclareTopic()],
            ['queue_bind', () => this.testQueueBind()],
            ['basic_publish_consume', () => this.testBasicPublishConsume()],
            ['publish_with_properties', () => this.testPublishWithProperties()],
            ['publish_with_headers', () => this.testPublishWithHeaders()],
            ['message_ordering', () => this.testMessageOrdering()],
            ['basic_nack_requeue', () => this.testBasicNackRequeue()],
            ['basic_qos', () => this.testBasicQos()],
            ['direct_routing', () => this.testDirectRouting()],
            ['fanout_routing', () => this.testFanoutRouting()],
        ];

        let passed = 0;
        let failed = 0;

        for (const [name, testFunc] of tests) {
            if (await this.runTest(name, testFunc)) {
                passed++;
            } else {
                failed++;
            }
        }

        console.log('\n' + '='.repeat(60));
        console.log(`Results: ${passed} passed, ${failed} failed`);
        console.log('='.repeat(60) + '\n');

        return { passed, failed, results: this.results };
    }
}

async function main() {
    const host = process.argv[2] || 'localhost';
    const port = parseInt(process.argv[3]) || 5672;

    const test = new AmqpComplianceTest(host, port);

    try {
        console.log(`Connecting to AMQP server at ${host}:${port}...`);
        await test.connect();
        const results = await test.runAllTests();

        console.log('\n--- JSON RESULTS ---');
        console.log(JSON.stringify(results));

        process.exit(results.failed === 0 ? 0 : 1);
    } catch (e) {
        console.error(`Connection failed: ${e.message}`);
        process.exit(1);
    } finally {
        await test.disconnect();
    }
}

main();
