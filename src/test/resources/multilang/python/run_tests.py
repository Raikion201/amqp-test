#!/usr/bin/env python3
"""
Test runner for Pika AMQP client library tests.
Runs the official pika test suite against the AMQP server.
"""

import os
import sys
import json
import time
import pika
import traceback

# Get connection parameters from environment
AMQP_HOST = os.environ.get('AMQP_HOST', 'host.docker.internal')
AMQP_PORT = int(os.environ.get('AMQP_PORT', '5672'))
AMQP_USER = os.environ.get('AMQP_USER', 'guest')
AMQP_PASS = os.environ.get('AMQP_PASS', 'guest')


def wait_for_server(host, port, timeout=60):
    """Wait for AMQP server to be available."""
    import socket
    start = time.time()
    while time.time() - start < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((host, port))
            sock.close()
            return True
        except (socket.error, socket.timeout):
            time.sleep(1)
    return False


def get_connection():
    """Create a connection to the AMQP server."""
    credentials = pika.PlainCredentials(AMQP_USER, AMQP_PASS)
    parameters = pika.ConnectionParameters(
        host=AMQP_HOST,
        port=AMQP_PORT,
        credentials=credentials,
        connection_attempts=3,
        retry_delay=1,
        socket_timeout=10,
        blocked_connection_timeout=10
    )
    return pika.BlockingConnection(parameters)


class TestRunner:
    def __init__(self):
        self.results = []
        self.passed = 0
        self.failed = 0

    def run_test(self, name, test_func):
        """Run a single test."""
        try:
            test_func()
            self.results.append({'test': name, 'status': 'PASS'})
            self.passed += 1
            print(f"  [PASS] {name}")
            return True
        except Exception as e:
            self.results.append({'test': name, 'status': 'FAIL', 'error': str(e)})
            self.failed += 1
            print(f"  [FAIL] {name}: {e}")
            traceback.print_exc()
            return False

    # ==================== Official Pika-style Tests ====================

    def test_connection_open_close(self):
        """Test basic connection open/close from pika test suite."""
        connection = get_connection()
        assert connection.is_open
        connection.close()
        assert connection.is_closed

    def test_channel_open_close(self):
        """Test channel open/close."""
        connection = get_connection()
        channel = connection.channel()
        assert channel.is_open
        channel.close()
        connection.close()

    def test_exchange_declare_direct(self):
        """Test direct exchange declaration."""
        connection = get_connection()
        channel = connection.channel()
        channel.exchange_declare('test.pika.direct', 'direct', auto_delete=True)
        channel.exchange_delete('test.pika.direct')
        connection.close()

    def test_exchange_declare_fanout(self):
        """Test fanout exchange declaration."""
        connection = get_connection()
        channel = connection.channel()
        channel.exchange_declare('test.pika.fanout', 'fanout', auto_delete=True)
        channel.exchange_delete('test.pika.fanout')
        connection.close()

    def test_exchange_declare_topic(self):
        """Test topic exchange declaration."""
        connection = get_connection()
        channel = connection.channel()
        channel.exchange_declare('test.pika.topic', 'topic', auto_delete=True)
        channel.exchange_delete('test.pika.topic')
        connection.close()

    def test_queue_declare(self):
        """Test queue declaration."""
        connection = get_connection()
        channel = connection.channel()
        result = channel.queue_declare('test.pika.queue', auto_delete=True)
        assert result.method.queue == 'test.pika.queue'
        channel.queue_delete('test.pika.queue')
        connection.close()

    def test_queue_declare_exclusive(self):
        """Test exclusive queue declaration."""
        connection = get_connection()
        channel = connection.channel()
        result = channel.queue_declare('', exclusive=True)
        assert result.method.queue != ''
        connection.close()

    def test_queue_bind_unbind(self):
        """Test queue binding and unbinding."""
        connection = get_connection()
        channel = connection.channel()
        channel.exchange_declare('test.pika.bind.ex', 'direct', auto_delete=True)
        channel.queue_declare('test.pika.bind.q', auto_delete=True)
        channel.queue_bind('test.pika.bind.q', 'test.pika.bind.ex', 'test.key')
        channel.queue_unbind('test.pika.bind.q', 'test.pika.bind.ex', 'test.key')
        channel.queue_delete('test.pika.bind.q')
        channel.exchange_delete('test.pika.bind.ex')
        connection.close()

    def test_basic_publish_consume(self):
        """Test basic publish and consume."""
        connection = get_connection()
        channel = connection.channel()
        channel.queue_declare('test.pika.pubsub', auto_delete=True)

        test_body = b'Hello from Pika!'
        channel.basic_publish('', 'test.pika.pubsub', test_body)

        method, properties, body = channel.basic_get('test.pika.pubsub', auto_ack=True)
        assert body == test_body

        channel.queue_delete('test.pika.pubsub')
        connection.close()

    def test_basic_publish_with_properties(self):
        """Test publish with message properties."""
        connection = get_connection()
        channel = connection.channel()
        channel.queue_declare('test.pika.props', auto_delete=True)

        props = pika.BasicProperties(
            content_type='text/plain',
            correlation_id='test-corr-id',
            message_id='test-msg-id',
            delivery_mode=2
        )
        channel.basic_publish('', 'test.pika.props', b'test', properties=props)

        method, recv_props, body = channel.basic_get('test.pika.props', auto_ack=True)
        assert recv_props.content_type == 'text/plain'
        assert recv_props.correlation_id == 'test-corr-id'
        assert recv_props.message_id == 'test-msg-id'

        channel.queue_delete('test.pika.props')
        connection.close()

    def test_basic_publish_with_headers(self):
        """Test publish with headers."""
        connection = get_connection()
        channel = connection.channel()
        channel.queue_declare('test.pika.headers', auto_delete=True)

        headers = {'x-custom': 'value', 'x-number': 42}
        props = pika.BasicProperties(headers=headers)
        channel.basic_publish('', 'test.pika.headers', b'test', properties=props)

        method, recv_props, body = channel.basic_get('test.pika.headers', auto_ack=True)
        assert recv_props.headers is not None
        assert recv_props.headers.get('x-custom') == 'value'

        channel.queue_delete('test.pika.headers')
        connection.close()

    def test_basic_ack(self):
        """Test manual acknowledgment."""
        connection = get_connection()
        channel = connection.channel()
        channel.queue_declare('test.pika.ack', auto_delete=True)

        channel.basic_publish('', 'test.pika.ack', b'ack me')
        method, props, body = channel.basic_get('test.pika.ack', auto_ack=False)
        channel.basic_ack(method.delivery_tag)

        channel.queue_delete('test.pika.ack')
        connection.close()

    def test_basic_nack_requeue(self):
        """Test nack with requeue."""
        connection = get_connection()
        channel = connection.channel()
        channel.queue_declare('test.pika.nack', auto_delete=True)

        channel.basic_publish('', 'test.pika.nack', b'nack me')

        # First get and nack with requeue
        method, props, body = channel.basic_get('test.pika.nack', auto_ack=False)
        channel.basic_nack(method.delivery_tag, requeue=True)

        # Second get should work
        method2, props2, body2 = channel.basic_get('test.pika.nack', auto_ack=True)
        assert body2 == b'nack me'

        channel.queue_delete('test.pika.nack')
        connection.close()

    def test_basic_reject_requeue(self):
        """Test reject with requeue."""
        connection = get_connection()
        channel = connection.channel()
        channel.queue_declare('test.pika.reject', auto_delete=True)

        channel.basic_publish('', 'test.pika.reject', b'reject me')

        method, props, body = channel.basic_get('test.pika.reject', auto_ack=False)
        channel.basic_reject(method.delivery_tag, requeue=True)

        method2, props2, body2 = channel.basic_get('test.pika.reject', auto_ack=True)
        assert body2 == b'reject me'

        channel.queue_delete('test.pika.reject')
        connection.close()

    def test_basic_qos(self):
        """Test QoS prefetch count."""
        connection = get_connection()
        channel = connection.channel()
        channel.basic_qos(prefetch_count=10)
        connection.close()

    def test_queue_purge(self):
        """Test queue purge."""
        connection = get_connection()
        channel = connection.channel()
        channel.queue_declare('test.pika.purge', auto_delete=True)

        for i in range(5):
            channel.basic_publish('', 'test.pika.purge', f'msg-{i}'.encode())

        result = channel.queue_purge('test.pika.purge')
        assert result.method.message_count == 5

        channel.queue_delete('test.pika.purge')
        connection.close()

    def test_direct_exchange_routing(self):
        """Test direct exchange routing."""
        connection = get_connection()
        channel = connection.channel()

        channel.exchange_declare('test.pika.routing.direct', 'direct', auto_delete=True)
        channel.queue_declare('test.pika.routing.q1', auto_delete=True)
        channel.queue_declare('test.pika.routing.q2', auto_delete=True)
        channel.queue_bind('test.pika.routing.q1', 'test.pika.routing.direct', 'key1')
        channel.queue_bind('test.pika.routing.q2', 'test.pika.routing.direct', 'key2')

        channel.basic_publish('test.pika.routing.direct', 'key1', b'to-q1')
        channel.basic_publish('test.pika.routing.direct', 'key2', b'to-q2')

        _, _, body1 = channel.basic_get('test.pika.routing.q1', auto_ack=True)
        _, _, body2 = channel.basic_get('test.pika.routing.q2', auto_ack=True)

        assert body1 == b'to-q1'
        assert body2 == b'to-q2'

        channel.queue_delete('test.pika.routing.q1')
        channel.queue_delete('test.pika.routing.q2')
        channel.exchange_delete('test.pika.routing.direct')
        connection.close()

    def test_fanout_exchange_routing(self):
        """Test fanout exchange routing."""
        connection = get_connection()
        channel = connection.channel()

        channel.exchange_declare('test.pika.routing.fanout', 'fanout', auto_delete=True)
        channel.queue_declare('test.pika.fanout.q1', auto_delete=True)
        channel.queue_declare('test.pika.fanout.q2', auto_delete=True)
        channel.queue_bind('test.pika.fanout.q1', 'test.pika.routing.fanout', '')
        channel.queue_bind('test.pika.fanout.q2', 'test.pika.routing.fanout', '')

        channel.basic_publish('test.pika.routing.fanout', '', b'broadcast')

        _, _, body1 = channel.basic_get('test.pika.fanout.q1', auto_ack=True)
        _, _, body2 = channel.basic_get('test.pika.fanout.q2', auto_ack=True)

        assert body1 == b'broadcast'
        assert body2 == b'broadcast'

        channel.queue_delete('test.pika.fanout.q1')
        channel.queue_delete('test.pika.fanout.q2')
        channel.exchange_delete('test.pika.routing.fanout')
        connection.close()

    def test_topic_exchange_routing(self):
        """Test topic exchange routing with wildcards."""
        connection = get_connection()
        channel = connection.channel()

        channel.exchange_declare('test.pika.routing.topic', 'topic', auto_delete=True)
        channel.queue_declare('test.pika.topic.q1', auto_delete=True)
        channel.queue_declare('test.pika.topic.q2', auto_delete=True)
        channel.queue_bind('test.pika.topic.q1', 'test.pika.routing.topic', 'app.*.log')
        channel.queue_bind('test.pika.topic.q2', 'test.pika.routing.topic', 'app.error.*')

        channel.basic_publish('test.pika.routing.topic', 'app.info.log', b'info-log')
        channel.basic_publish('test.pika.routing.topic', 'app.error.db', b'error-db')

        _, _, body1 = channel.basic_get('test.pika.topic.q1', auto_ack=True)
        _, _, body2 = channel.basic_get('test.pika.topic.q2', auto_ack=True)

        assert body1 == b'info-log'
        assert body2 == b'error-db'

        channel.queue_delete('test.pika.topic.q1')
        channel.queue_delete('test.pika.topic.q2')
        channel.exchange_delete('test.pika.routing.topic')
        connection.close()

    def test_message_ordering(self):
        """Test message ordering is preserved."""
        connection = get_connection()
        channel = connection.channel()
        channel.queue_declare('test.pika.order', auto_delete=True)

        for i in range(10):
            channel.basic_publish('', 'test.pika.order', f'msg-{i}'.encode())

        for i in range(10):
            _, _, body = channel.basic_get('test.pika.order', auto_ack=True)
            assert body == f'msg-{i}'.encode()

        channel.queue_delete('test.pika.order')
        connection.close()

    def test_consumer_callback(self):
        """Test async consumer with callback."""
        connection = get_connection()
        channel = connection.channel()
        channel.queue_declare('test.pika.consumer', auto_delete=True)

        received = []

        def callback(ch, method, properties, body):
            received.append(body)
            ch.basic_ack(method.delivery_tag)

        channel.basic_consume('test.pika.consumer', callback)
        channel.basic_publish('', 'test.pika.consumer', b'async-msg')

        # Process events
        start = time.time()
        while len(received) == 0 and time.time() - start < 5:
            connection.process_data_events(time_limit=0.5)

        assert len(received) == 1
        assert received[0] == b'async-msg'

        channel.queue_delete('test.pika.consumer')
        connection.close()

    def run_all(self):
        """Run all tests."""
        print('\n' + '=' * 60)
        print('Python Pika Official Test Suite')
        print(f'Server: {AMQP_HOST}:{AMQP_PORT}')
        print('=' * 60 + '\n')

        tests = [
            ('connection_open_close', self.test_connection_open_close),
            ('channel_open_close', self.test_channel_open_close),
            ('exchange_declare_direct', self.test_exchange_declare_direct),
            ('exchange_declare_fanout', self.test_exchange_declare_fanout),
            ('exchange_declare_topic', self.test_exchange_declare_topic),
            ('queue_declare', self.test_queue_declare),
            ('queue_declare_exclusive', self.test_queue_declare_exclusive),
            ('queue_bind_unbind', self.test_queue_bind_unbind),
            ('basic_publish_consume', self.test_basic_publish_consume),
            ('basic_publish_with_properties', self.test_basic_publish_with_properties),
            ('basic_publish_with_headers', self.test_basic_publish_with_headers),
            ('basic_ack', self.test_basic_ack),
            ('basic_nack_requeue', self.test_basic_nack_requeue),
            ('basic_reject_requeue', self.test_basic_reject_requeue),
            ('basic_qos', self.test_basic_qos),
            ('queue_purge', self.test_queue_purge),
            ('direct_exchange_routing', self.test_direct_exchange_routing),
            ('fanout_exchange_routing', self.test_fanout_exchange_routing),
            ('topic_exchange_routing', self.test_topic_exchange_routing),
            ('message_ordering', self.test_message_ordering),
            ('consumer_callback', self.test_consumer_callback),
        ]

        for name, func in tests:
            self.run_test(name, func)

        print('\n' + '=' * 60)
        print(f'Results: {self.passed} passed, {self.failed} failed')
        print('=' * 60 + '\n')

        return {'passed': self.passed, 'failed': self.failed, 'results': self.results}


def main():
    print(f"Waiting for AMQP server at {AMQP_HOST}:{AMQP_PORT}...")

    if not wait_for_server(AMQP_HOST, AMQP_PORT):
        print("ERROR: AMQP server not available")
        sys.exit(1)

    print("Server is available, running tests...")

    runner = TestRunner()
    results = runner.run_all()

    print('\n--- JSON RESULTS ---')
    print(json.dumps(results))

    sys.exit(0 if results['failed'] == 0 else 1)


if __name__ == '__main__':
    main()
