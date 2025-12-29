#!/usr/bin/env python3
"""
AMQP 0-9-1 Compliance Test using Python Pika library.
Tests basic AMQP operations against the server.
"""

import pika
import sys
import json
import time
import uuid
import traceback

class AmqpComplianceTest:
    def __init__(self, host='localhost', port=5672):
        self.host = host
        self.port = port
        self.results = []
        self.connection = None
        self.channel = None

    def connect(self):
        """Establish connection to AMQP server."""
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=credentials,
            connection_attempts=3,
            retry_delay=1,
            socket_timeout=10
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

    def disconnect(self):
        """Close connection."""
        if self.connection and self.connection.is_open:
            self.connection.close()

    def run_test(self, name, test_func):
        """Run a single test and record result."""
        try:
            test_func()
            self.results.append({'test': name, 'status': 'PASS', 'error': None})
            print(f"  [PASS] {name}")
            return True
        except Exception as e:
            self.results.append({'test': name, 'status': 'FAIL', 'error': str(e)})
            print(f"  [FAIL] {name}: {e}")
            traceback.print_exc()
            return False

    # ==================== Connection Tests ====================

    def test_connection_open(self):
        """Test that connection opens successfully."""
        assert self.connection.is_open, "Connection should be open"

    def test_channel_open(self):
        """Test that channel opens successfully."""
        assert self.channel.is_open, "Channel should be open"

    # ==================== Queue Tests ====================

    def test_queue_declare(self):
        """Test queue declaration."""
        queue_name = f"test.python.queue.{uuid.uuid4().hex[:8]}"
        result = self.channel.queue_declare(queue=queue_name, auto_delete=True)
        assert result.method.queue == queue_name

    def test_queue_declare_anonymous(self):
        """Test anonymous queue declaration."""
        result = self.channel.queue_declare(queue='', exclusive=True)
        assert result.method.queue != ''

    # ==================== Exchange Tests ====================

    def test_exchange_declare_direct(self):
        """Test direct exchange declaration."""
        exchange_name = f"test.python.direct.{uuid.uuid4().hex[:8]}"
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct', auto_delete=True)

    def test_exchange_declare_fanout(self):
        """Test fanout exchange declaration."""
        exchange_name = f"test.python.fanout.{uuid.uuid4().hex[:8]}"
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', auto_delete=True)

    def test_exchange_declare_topic(self):
        """Test topic exchange declaration."""
        exchange_name = f"test.python.topic.{uuid.uuid4().hex[:8]}"
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='topic', auto_delete=True)

    # ==================== Binding Tests ====================

    def test_queue_bind(self):
        """Test queue binding to exchange."""
        exchange_name = f"test.python.bind.ex.{uuid.uuid4().hex[:8]}"
        queue_name = f"test.python.bind.q.{uuid.uuid4().hex[:8]}"

        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct', auto_delete=True)
        self.channel.queue_declare(queue=queue_name, auto_delete=True)
        self.channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key='test.key')

    # ==================== Publish/Consume Tests ====================

    def test_basic_publish_consume(self):
        """Test basic message publish and consume."""
        queue_name = f"test.python.pubsub.{uuid.uuid4().hex[:8]}"
        test_message = f"Hello from Python! {uuid.uuid4().hex}"
        received = []

        self.channel.queue_declare(queue=queue_name, auto_delete=True)
        self.channel.basic_publish(exchange='', routing_key=queue_name, body=test_message)

        def callback(ch, method, properties, body):
            received.append(body.decode())
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

        # Process one message with timeout
        start = time.time()
        while len(received) == 0 and time.time() - start < 5:
            self.connection.process_data_events(time_limit=0.5)

        assert len(received) == 1, f"Expected 1 message, got {len(received)}"
        assert received[0] == test_message, f"Message mismatch: {received[0]} != {test_message}"

    def test_publish_with_properties(self):
        """Test message publish with properties."""
        queue_name = f"test.python.props.{uuid.uuid4().hex[:8]}"
        received_props = [None]

        self.channel.queue_declare(queue=queue_name, auto_delete=True)

        properties = pika.BasicProperties(
            content_type='text/plain',
            correlation_id='corr-123',
            message_id='msg-456',
            priority=5
        )
        self.channel.basic_publish(exchange='', routing_key=queue_name, body='test', properties=properties)

        def callback(ch, method, props, body):
            received_props[0] = props
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

        start = time.time()
        while received_props[0] is None and time.time() - start < 5:
            self.connection.process_data_events(time_limit=0.5)

        assert received_props[0] is not None, "Should receive message properties"
        assert received_props[0].content_type == 'text/plain'
        assert received_props[0].correlation_id == 'corr-123'
        assert received_props[0].message_id == 'msg-456'

    def test_publish_with_headers(self):
        """Test message publish with headers."""
        queue_name = f"test.python.headers.{uuid.uuid4().hex[:8]}"
        received_headers = [None]

        self.channel.queue_declare(queue=queue_name, auto_delete=True)

        headers = {'x-custom-header': 'custom-value', 'x-number': 42}
        properties = pika.BasicProperties(headers=headers)
        self.channel.basic_publish(exchange='', routing_key=queue_name, body='test', properties=properties)

        def callback(ch, method, props, body):
            received_headers[0] = props.headers
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

        start = time.time()
        while received_headers[0] is None and time.time() - start < 5:
            self.connection.process_data_events(time_limit=0.5)

        assert received_headers[0] is not None, "Should receive headers"
        assert received_headers[0].get('x-custom-header') == 'custom-value'

    def test_message_ordering(self):
        """Test that messages are delivered in order."""
        queue_name = f"test.python.order.{uuid.uuid4().hex[:8]}"
        message_count = 10
        received = []

        self.channel.queue_declare(queue=queue_name, auto_delete=True)

        for i in range(message_count):
            self.channel.basic_publish(exchange='', routing_key=queue_name, body=f"Message-{i}")

        def callback(ch, method, props, body):
            received.append(body.decode())
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

        start = time.time()
        while len(received) < message_count and time.time() - start < 10:
            self.connection.process_data_events(time_limit=0.5)

        assert len(received) == message_count
        for i in range(message_count):
            assert received[i] == f"Message-{i}", f"Out of order at {i}"

    # ==================== Acknowledgment Tests ====================

    def test_basic_nack_requeue(self):
        """Test nack with requeue."""
        queue_name = f"test.python.nack.{uuid.uuid4().hex[:8]}"
        delivery_count = [0]

        self.channel.queue_declare(queue=queue_name, auto_delete=True)
        self.channel.basic_publish(exchange='', routing_key=queue_name, body='nack-me')

        def callback(ch, method, props, body):
            delivery_count[0] += 1
            if delivery_count[0] == 1:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

        start = time.time()
        while delivery_count[0] < 2 and time.time() - start < 5:
            self.connection.process_data_events(time_limit=0.5)

        assert delivery_count[0] == 2, f"Expected 2 deliveries, got {delivery_count[0]}"

    # ==================== QoS Tests ====================

    def test_basic_qos(self):
        """Test prefetch count."""
        queue_name = f"test.python.qos.{uuid.uuid4().hex[:8]}"

        self.channel.queue_declare(queue=queue_name, auto_delete=True)
        self.channel.basic_qos(prefetch_count=5)

        for i in range(10):
            self.channel.basic_publish(exchange='', routing_key=queue_name, body=f"Message-{i}")

    # ==================== Exchange Routing Tests ====================

    def test_direct_routing(self):
        """Test direct exchange routing."""
        exchange_name = f"test.python.routing.direct.{uuid.uuid4().hex[:8]}"
        queue1 = f"test.python.routing.q1.{uuid.uuid4().hex[:8]}"
        queue2 = f"test.python.routing.q2.{uuid.uuid4().hex[:8]}"
        received_q1 = []
        received_q2 = []

        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct', auto_delete=True)
        self.channel.queue_declare(queue=queue1, auto_delete=True)
        self.channel.queue_declare(queue=queue2, auto_delete=True)
        self.channel.queue_bind(queue=queue1, exchange=exchange_name, routing_key='key1')
        self.channel.queue_bind(queue=queue2, exchange=exchange_name, routing_key='key2')

        self.channel.basic_publish(exchange=exchange_name, routing_key='key1', body='to-q1')
        self.channel.basic_publish(exchange=exchange_name, routing_key='key2', body='to-q2')

        self.channel.basic_consume(queue=queue1, on_message_callback=lambda ch, m, p, b: received_q1.append(b.decode()) or ch.basic_ack(m.delivery_tag), auto_ack=False)
        self.channel.basic_consume(queue=queue2, on_message_callback=lambda ch, m, p, b: received_q2.append(b.decode()) or ch.basic_ack(m.delivery_tag), auto_ack=False)

        start = time.time()
        while (len(received_q1) < 1 or len(received_q2) < 1) and time.time() - start < 5:
            self.connection.process_data_events(time_limit=0.5)

        assert received_q1 == ['to-q1']
        assert received_q2 == ['to-q2']

    def test_fanout_routing(self):
        """Test fanout exchange routing."""
        exchange_name = f"test.python.routing.fanout.{uuid.uuid4().hex[:8]}"
        queue1 = f"test.python.fanout.q1.{uuid.uuid4().hex[:8]}"
        queue2 = f"test.python.fanout.q2.{uuid.uuid4().hex[:8]}"
        received_q1 = []
        received_q2 = []

        self.channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', auto_delete=True)
        self.channel.queue_declare(queue=queue1, auto_delete=True)
        self.channel.queue_declare(queue=queue2, auto_delete=True)
        self.channel.queue_bind(queue=queue1, exchange=exchange_name, routing_key='')
        self.channel.queue_bind(queue=queue2, exchange=exchange_name, routing_key='')

        self.channel.basic_publish(exchange=exchange_name, routing_key='', body='broadcast')

        self.channel.basic_consume(queue=queue1, on_message_callback=lambda ch, m, p, b: received_q1.append(b.decode()) or ch.basic_ack(m.delivery_tag), auto_ack=False)
        self.channel.basic_consume(queue=queue2, on_message_callback=lambda ch, m, p, b: received_q2.append(b.decode()) or ch.basic_ack(m.delivery_tag), auto_ack=False)

        start = time.time()
        while (len(received_q1) < 1 or len(received_q2) < 1) and time.time() - start < 5:
            self.connection.process_data_events(time_limit=0.5)

        assert received_q1 == ['broadcast']
        assert received_q2 == ['broadcast']

    def run_all_tests(self):
        """Run all compliance tests."""
        print(f"\n{'='*60}")
        print("Python Pika AMQP 0-9-1 Compliance Tests")
        print(f"Server: {self.host}:{self.port}")
        print(f"{'='*60}\n")

        tests = [
            ('connection_open', self.test_connection_open),
            ('channel_open', self.test_channel_open),
            ('queue_declare', self.test_queue_declare),
            ('queue_declare_anonymous', self.test_queue_declare_anonymous),
            ('exchange_declare_direct', self.test_exchange_declare_direct),
            ('exchange_declare_fanout', self.test_exchange_declare_fanout),
            ('exchange_declare_topic', self.test_exchange_declare_topic),
            ('queue_bind', self.test_queue_bind),
            ('basic_publish_consume', self.test_basic_publish_consume),
            ('publish_with_properties', self.test_publish_with_properties),
            ('publish_with_headers', self.test_publish_with_headers),
            ('message_ordering', self.test_message_ordering),
            ('basic_nack_requeue', self.test_basic_nack_requeue),
            ('basic_qos', self.test_basic_qos),
            ('direct_routing', self.test_direct_routing),
            ('fanout_routing', self.test_fanout_routing),
        ]

        passed = 0
        failed = 0

        for name, test_func in tests:
            if self.run_test(name, test_func):
                passed += 1
            else:
                failed += 1

        print(f"\n{'='*60}")
        print(f"Results: {passed} passed, {failed} failed")
        print(f"{'='*60}\n")

        return {'passed': passed, 'failed': failed, 'results': self.results}


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5672

    test = AmqpComplianceTest(host, port)

    try:
        print(f"Connecting to AMQP server at {host}:{port}...")
        test.connect()
        results = test.run_all_tests()

        # Output JSON results for parsing
        print("\n--- JSON RESULTS ---")
        print(json.dumps(results))

        sys.exit(0 if results['failed'] == 0 else 1)
    except Exception as e:
        print(f"Connection failed: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        test.disconnect()


if __name__ == '__main__':
    main()
