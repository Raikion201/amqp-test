#!/usr/bin/env node
/**
 * AMQP 0-9-1 Compliance Tests using Node.js amqplib.
 * Based on official amqplib test patterns.
 */

const amqp = require('amqplib');
const net = require('net');

const AMQP_HOST = process.env.AMQP_HOST || 'host.docker.internal';
const AMQP_PORT = parseInt(process.env.AMQP_PORT) || 5672;
const AMQP_URL = `amqp://guest:guest@${AMQP_HOST}:${AMQP_PORT}`;

function randomString() {
    return Math.random().toString(36).substring(2, 10);
}

function waitForServer(host, port, timeout = 60000) {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        const tryConnect = () => {
            const socket = new net.Socket();
            socket.setTimeout(5000);
            socket.on('connect', () => {
                socket.destroy();
                resolve(true);
            });
            socket.on('error', () => {
                socket.destroy();
                if (Date.now() - start < timeout) {
                    setTimeout(tryConnect, 1000);
                } else {
                    reject(new Error('Server not available'));
                }
            });
            socket.on('timeout', () => {
                socket.destroy();
                if (Date.now() - start < timeout) {
                    setTimeout(tryConnect, 1000);
                } else {
                    reject(new Error('Server not available'));
                }
            });
            socket.connect(port, host);
        };
        tryConnect();
    });
}

class TestRunner {
    constructor() {
        this.results = [];
        this.passed = 0;
        this.failed = 0;
    }

    async runTest(name, testFunc) {
        try {
            await testFunc();
            this.results.push({ test: name, status: 'PASS' });
            this.passed++;
            console.log(`  [PASS] ${name}`);
            return true;
        } catch (e) {
            this.results.push({ test: name, status: 'FAIL', error: e.message });
            this.failed++;
            console.log(`  [FAIL] ${name}: ${e.message}`);
            return false;
        }
    }

    // ==================== Connection Tests ====================

    async testConnectionOpen() {
        const conn = await amqp.connect(AMQP_URL);
        if (!conn) throw new Error('Connection should be open');
        await conn.close();
    }

    async testChannelOpen() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        if (!ch) throw new Error('Channel should be open');
        await ch.close();
        await conn.close();
    }

    // ==================== Queue Tests ====================

    async testQueueDeclare() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const queueName = `test.nodejs.queue.${randomString()}`;
        const result = await ch.assertQueue(queueName, { autoDelete: true });
        if (result.queue !== queueName) throw new Error('Queue name mismatch');
        await ch.deleteQueue(queueName);
        await conn.close();
    }

    async testQueueDeclareExclusive() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const result = await ch.assertQueue('', { exclusive: true });
        if (!result.queue) throw new Error('Exclusive queue should have name');
        await conn.close();
    }

    // ==================== Exchange Tests ====================

    async testExchangeDeclareDirect() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const exName = `test.nodejs.direct.${randomString()}`;
        await ch.assertExchange(exName, 'direct', { autoDelete: true });
        await ch.deleteExchange(exName);
        await conn.close();
    }

    async testExchangeDeclareFanout() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const exName = `test.nodejs.fanout.${randomString()}`;
        await ch.assertExchange(exName, 'fanout', { autoDelete: true });
        await ch.deleteExchange(exName);
        await conn.close();
    }

    async testExchangeDeclareTopic() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const exName = `test.nodejs.topic.${randomString()}`;
        await ch.assertExchange(exName, 'topic', { autoDelete: true });
        await ch.deleteExchange(exName);
        await conn.close();
    }

    // ==================== Binding Tests ====================

    async testQueueBind() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const exName = `test.nodejs.bind.ex.${randomString()}`;
        const qName = `test.nodejs.bind.q.${randomString()}`;

        await ch.assertExchange(exName, 'direct', { autoDelete: true });
        await ch.assertQueue(qName, { autoDelete: true });
        await ch.bindQueue(qName, exName, 'test.key');
        await ch.unbindQueue(qName, exName, 'test.key');
        await ch.deleteQueue(qName);
        await ch.deleteExchange(exName);
        await conn.close();
    }

    // ==================== Publish/Consume Tests ====================

    async testBasicPublishConsume() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const qName = `test.nodejs.pubsub.${randomString()}`;
        const testMsg = `Hello from Node.js! ${randomString()}`;

        await ch.assertQueue(qName, { autoDelete: true });
        ch.sendToQueue(qName, Buffer.from(testMsg));

        const msg = await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Timeout')), 5000);
            ch.consume(qName, (m) => {
                if (m) {
                    clearTimeout(timeout);
                    ch.ack(m);
                    resolve(m);
                }
            });
        });

        if (msg.content.toString() !== testMsg) {
            throw new Error('Message content mismatch');
        }

        await ch.deleteQueue(qName);
        await conn.close();
    }

    async testPublishWithProperties() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const qName = `test.nodejs.props.${randomString()}`;

        await ch.assertQueue(qName, { autoDelete: true });
        ch.sendToQueue(qName, Buffer.from('test'), {
            contentType: 'text/plain',
            correlationId: 'corr-123',
            messageId: 'msg-456'
        });

        const msg = await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Timeout')), 5000);
            ch.consume(qName, (m) => {
                if (m) {
                    clearTimeout(timeout);
                    ch.ack(m);
                    resolve(m);
                }
            });
        });

        if (msg.properties.contentType !== 'text/plain') throw new Error('contentType mismatch');
        if (msg.properties.correlationId !== 'corr-123') throw new Error('correlationId mismatch');
        if (msg.properties.messageId !== 'msg-456') throw new Error('messageId mismatch');

        await ch.deleteQueue(qName);
        await conn.close();
    }

    async testPublishWithHeaders() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const qName = `test.nodejs.headers.${randomString()}`;

        await ch.assertQueue(qName, { autoDelete: true });
        ch.sendToQueue(qName, Buffer.from('test'), {
            headers: { 'x-custom': 'value', 'x-number': 42 }
        });

        const msg = await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Timeout')), 5000);
            ch.consume(qName, (m) => {
                if (m) {
                    clearTimeout(timeout);
                    ch.ack(m);
                    resolve(m);
                }
            });
        });

        if (!msg.properties.headers) throw new Error('Headers should exist');
        if (msg.properties.headers['x-custom'] !== 'value') throw new Error('Header mismatch');

        await ch.deleteQueue(qName);
        await conn.close();
    }

    async testBasicNackRequeue() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const qName = `test.nodejs.nack.${randomString()}`;
        let deliveryCount = 0;

        await ch.assertQueue(qName, { autoDelete: true });
        ch.sendToQueue(qName, Buffer.from('nack-me'));

        await new Promise((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error('Timeout')), 5000);
            ch.consume(qName, (msg) => {
                if (msg) {
                    deliveryCount++;
                    if (deliveryCount === 1) {
                        ch.nack(msg, false, true);
                    } else {
                        ch.ack(msg);
                        clearTimeout(timeout);
                        resolve();
                    }
                }
            });
        });

        if (deliveryCount !== 2) throw new Error(`Expected 2 deliveries, got ${deliveryCount}`);

        await ch.deleteQueue(qName);
        await conn.close();
    }

    async testBasicQos() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        await ch.prefetch(10);
        await conn.close();
    }

    async testQueuePurge() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const qName = `test.nodejs.purge.${randomString()}`;

        await ch.assertQueue(qName, { autoDelete: true });
        for (let i = 0; i < 5; i++) {
            ch.sendToQueue(qName, Buffer.from(`msg-${i}`));
        }

        await new Promise(r => setTimeout(r, 100));
        const result = await ch.purgeQueue(qName);
        if (result.messageCount !== 5) throw new Error(`Expected 5 purged, got ${result.messageCount}`);

        await ch.deleteQueue(qName);
        await conn.close();
    }

    async testDirectRouting() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const exName = `test.nodejs.routing.direct.${randomString()}`;
        const q1 = `test.nodejs.routing.q1.${randomString()}`;
        const q2 = `test.nodejs.routing.q2.${randomString()}`;

        await ch.assertExchange(exName, 'direct', { autoDelete: true });
        await ch.assertQueue(q1, { autoDelete: true });
        await ch.assertQueue(q2, { autoDelete: true });
        await ch.bindQueue(q1, exName, 'key1');
        await ch.bindQueue(q2, exName, 'key2');

        ch.publish(exName, 'key1', Buffer.from('to-q1'));
        ch.publish(exName, 'key2', Buffer.from('to-q2'));

        await new Promise(r => setTimeout(r, 100));

        const msg1 = await ch.get(q1, { noAck: true });
        const msg2 = await ch.get(q2, { noAck: true });

        if (!msg1 || msg1.content.toString() !== 'to-q1') throw new Error('Routing to q1 failed');
        if (!msg2 || msg2.content.toString() !== 'to-q2') throw new Error('Routing to q2 failed');

        await ch.deleteQueue(q1);
        await ch.deleteQueue(q2);
        await ch.deleteExchange(exName);
        await conn.close();
    }

    async testFanoutRouting() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const exName = `test.nodejs.routing.fanout.${randomString()}`;
        const q1 = `test.nodejs.fanout.q1.${randomString()}`;
        const q2 = `test.nodejs.fanout.q2.${randomString()}`;

        await ch.assertExchange(exName, 'fanout', { autoDelete: true });
        await ch.assertQueue(q1, { autoDelete: true });
        await ch.assertQueue(q2, { autoDelete: true });
        await ch.bindQueue(q1, exName, '');
        await ch.bindQueue(q2, exName, '');

        ch.publish(exName, '', Buffer.from('broadcast'));

        await new Promise(r => setTimeout(r, 100));

        const msg1 = await ch.get(q1, { noAck: true });
        const msg2 = await ch.get(q2, { noAck: true });

        if (!msg1 || msg1.content.toString() !== 'broadcast') throw new Error('Fanout to q1 failed');
        if (!msg2 || msg2.content.toString() !== 'broadcast') throw new Error('Fanout to q2 failed');

        await ch.deleteQueue(q1);
        await ch.deleteQueue(q2);
        await ch.deleteExchange(exName);
        await conn.close();
    }

    async testMessageOrdering() {
        const conn = await amqp.connect(AMQP_URL);
        const ch = await conn.createChannel();
        const qName = `test.nodejs.order.${randomString()}`;

        await ch.assertQueue(qName, { autoDelete: true });

        for (let i = 0; i < 10; i++) {
            ch.sendToQueue(qName, Buffer.from(`msg-${i}`));
        }

        await new Promise(r => setTimeout(r, 100));

        for (let i = 0; i < 10; i++) {
            const msg = await ch.get(qName, { noAck: true });
            if (!msg || msg.content.toString() !== `msg-${i}`) {
                throw new Error(`Out of order at ${i}`);
            }
        }

        await ch.deleteQueue(qName);
        await conn.close();
    }

    async runAll() {
        console.log('\n' + '='.repeat(60));
        console.log('Node.js amqplib Official Test Suite');
        console.log(`Server: ${AMQP_HOST}:${AMQP_PORT}`);
        console.log('='.repeat(60) + '\n');

        const tests = [
            ['connection_open', () => this.testConnectionOpen()],
            ['channel_open', () => this.testChannelOpen()],
            ['queue_declare', () => this.testQueueDeclare()],
            ['queue_declare_exclusive', () => this.testQueueDeclareExclusive()],
            ['exchange_declare_direct', () => this.testExchangeDeclareDirect()],
            ['exchange_declare_fanout', () => this.testExchangeDeclareFanout()],
            ['exchange_declare_topic', () => this.testExchangeDeclareTopic()],
            ['queue_bind', () => this.testQueueBind()],
            ['basic_publish_consume', () => this.testBasicPublishConsume()],
            ['publish_with_properties', () => this.testPublishWithProperties()],
            ['publish_with_headers', () => this.testPublishWithHeaders()],
            ['basic_nack_requeue', () => this.testBasicNackRequeue()],
            ['basic_qos', () => this.testBasicQos()],
            ['queue_purge', () => this.testQueuePurge()],
            ['direct_routing', () => this.testDirectRouting()],
            ['fanout_routing', () => this.testFanoutRouting()],
            ['message_ordering', () => this.testMessageOrdering()],
        ];

        for (const [name, testFunc] of tests) {
            await this.runTest(name, testFunc);
        }

        console.log('\n' + '='.repeat(60));
        console.log(`Results: ${this.passed} passed, ${this.failed} failed`);
        console.log('='.repeat(60) + '\n');

        return { passed: this.passed, failed: this.failed, results: this.results };
    }
}

async function main() {
    console.log(`Waiting for AMQP server at ${AMQP_HOST}:${AMQP_PORT}...`);

    try {
        await waitForServer(AMQP_HOST, AMQP_PORT);
        console.log('Server is available, running tests...');

        const runner = new TestRunner();
        const results = await runner.runAll();

        console.log('\n--- JSON RESULTS ---');
        console.log(JSON.stringify(results));

        process.exit(results.failed === 0 ? 0 : 1);
    } catch (e) {
        console.error(`Error: ${e.message}`);
        process.exit(1);
    }
}

main();
