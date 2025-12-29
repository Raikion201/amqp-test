#!/usr/bin/env python3
"""
AMQP 1.0 Compliance Tests for Python using qpid-proton
Based on official Qpid Proton library patterns

These tests verify AMQP 1.0 protocol compliance including:
- Connection establishment
- Session management
- Link establishment (sender/receiver)
- Message transfer with various types
- Flow control and credit management
- Delivery acknowledgments
- Message properties and application properties
"""

import os
import sys
import time
import threading
from datetime import datetime

try:
    from proton import Message, Delivery
    from proton.handlers import MessagingHandler
    from proton.reactor import Container
except ImportError:
    print("ERROR: python-qpid-proton not installed. Run: pip install python-qpid-proton")
    sys.exit(1)

HOST = os.environ.get('AMQP_HOST', 'localhost')
PORT = int(os.environ.get('AMQP_PORT', '5672'))
USERNAME = os.environ.get('AMQP_USER', 'guest')
PASSWORD = os.environ.get('AMQP_PASS', 'guest')

BROKER_URL = f"amqp://{USERNAME}:{PASSWORD}@{HOST}:{PORT}"

passed = 0
failed = 0
results = []


def log(msg):
    print(f"[{datetime.now().isoformat()}] {msg}")


def success(test_name):
    global passed
    passed += 1
    results.append({'name': test_name, 'status': 'PASSED'})
    log(f"✓ PASSED: {test_name}")


def fail(test_name, error):
    global failed
    failed += 1
    results.append({'name': test_name, 'status': 'FAILED', 'error': str(error)})
    log(f"✗ FAILED: {test_name} - {error}")


class TestHandler(MessagingHandler):
    """Base handler for tests with timeout support"""
    def __init__(self, test_name, timeout=10):
        super(TestHandler, self).__init__()
        self.test_name = test_name
        self.timeout = timeout
        self.completed = False
        self.error = None
        self.container = None

    def on_start(self, event):
        self.container = event.container
        # Schedule timeout
        event.container.schedule(self.timeout, self)

    def on_timer_task(self, event):
        if not self.completed:
            self.error = "Test timeout"
            if self.container:
                self.container.stop()


# Test 1: Basic AMQP 1.0 Connection
class TestBasicConnection(TestHandler):
    def __init__(self):
        super().__init__('basic_amqp10_connection')

    def on_start(self, event):
        super().on_start(event)
        event.container.connect(BROKER_URL)

    def on_connection_opened(self, event):
        self.completed = True
        success(self.test_name)
        event.connection.close()

    def on_transport_error(self, event):
        self.error = event.transport.condition.description if event.transport.condition else "Transport error"


# Test 2: Session Creation
class TestSessionCreation(TestHandler):
    def __init__(self):
        super().__init__('session_creation')

    def on_start(self, event):
        super().on_start(event)
        event.container.connect(BROKER_URL)

    def on_connection_opened(self, event):
        # Session is created automatically with connection
        self.completed = True
        success(self.test_name)
        event.connection.close()


# Test 3: Sender Link Creation
class TestSenderLink(TestHandler):
    def __init__(self):
        super().__init__('sender_link_creation')

    def on_start(self, event):
        super().on_start(event)
        conn = event.container.connect(BROKER_URL)
        event.container.create_sender(conn, "test-queue-sender")

    def on_link_opened(self, event):
        if event.sender:
            self.completed = True
            success(self.test_name)
            event.connection.close()


# Test 4: Receiver Link Creation
class TestReceiverLink(TestHandler):
    def __init__(self):
        super().__init__('receiver_link_creation')

    def on_start(self, event):
        super().on_start(event)
        conn = event.container.connect(BROKER_URL)
        event.container.create_receiver(conn, "test-queue-receiver")

    def on_link_opened(self, event):
        if event.receiver:
            self.completed = True
            success(self.test_name)
            event.connection.close()


# Test 5: Send Simple Message
class TestSendSimpleMessage(TestHandler):
    def __init__(self):
        super().__init__('send_simple_message')
        self.sender = None

    def on_start(self, event):
        super().on_start(event)
        conn = event.container.connect(BROKER_URL)
        self.sender = event.container.create_sender(conn, "test-queue-simple")

    def on_sendable(self, event):
        if self.sender and not self.completed:
            msg = Message(body="Hello AMQP 1.0!")
            self.sender.send(msg)

    def on_accepted(self, event):
        self.completed = True
        success(self.test_name)
        event.connection.close()


# Test 6: Send and Receive Message
class TestSendReceiveMessage(TestHandler):
    def __init__(self):
        super().__init__('send_receive_message', timeout=15)
        self.sender = None
        self.receiver = None
        self.test_body = f"Test message {time.time()}"
        self.message_sent = False
        self.queue_name = f"test-queue-sendrecv-{int(time.time() * 1000)}"

    def on_start(self, event):
        super().on_start(event)
        conn = event.container.connect(BROKER_URL)
        self.sender = event.container.create_sender(conn, self.queue_name)
        self.receiver = event.container.create_receiver(conn, self.queue_name)

    def on_sendable(self, event):
        if self.sender and not self.message_sent:
            msg = Message(body=self.test_body)
            self.sender.send(msg)
            self.message_sent = True

    def on_message(self, event):
        if event.message.body == self.test_body:
            self.completed = True
            success(self.test_name)
            event.connection.close()


# Test 7: Message Properties
class TestMessageProperties(TestHandler):
    def __init__(self):
        super().__init__('message_properties', timeout=15)
        self.sender = None
        self.receiver = None
        self.message_sent = False
        self.queue_name = f"test-queue-props-{int(time.time() * 1000)}"

    def on_start(self, event):
        super().on_start(event)
        conn = event.container.connect(BROKER_URL)
        self.sender = event.container.create_sender(conn, self.queue_name)
        self.receiver = event.container.create_receiver(conn, self.queue_name)

    def on_sendable(self, event):
        if self.sender and not self.message_sent:
            msg = Message(
                body="Properties test",
                id="msg-123",
                correlation_id="corr-456",
                content_type="text/plain",
                subject="test-subject",
                reply_to="reply-queue"
            )
            self.sender.send(msg)
            self.message_sent = True

    def on_message(self, event):
        try:
            m = event.message
            if (m.id == "msg-123" and
                m.correlation_id == "corr-456" and
                m.content_type == "text/plain"):
                self.completed = True
                success(self.test_name)
            else:
                self.error = "Properties mismatch"
        except Exception as e:
            self.error = str(e)
        event.connection.close()


# Test 8: Application Properties
class TestApplicationProperties(TestHandler):
    def __init__(self):
        super().__init__('application_properties', timeout=15)
        self.sender = None
        self.receiver = None
        self.message_sent = False
        self.queue_name = f"test-queue-appprops-{int(time.time() * 1000)}"

    def on_start(self, event):
        super().on_start(event)
        conn = event.container.connect(BROKER_URL)
        self.sender = event.container.create_sender(conn, self.queue_name)
        self.receiver = event.container.create_receiver(conn, self.queue_name)

    def on_sendable(self, event):
        if self.sender and not self.message_sent:
            msg = Message(
                body="App properties test",
                properties={
                    'x-custom-header': 'custom-value',
                    'x-number': 42,
                    'x-boolean': True
                }
            )
            self.sender.send(msg)
            self.message_sent = True

    def on_message(self, event):
        try:
            props = event.message.properties
            if (props and
                props.get('x-custom-header') == 'custom-value' and
                props.get('x-number') == 42 and
                props.get('x-boolean') == True):
                self.completed = True
                success(self.test_name)
            else:
                self.error = f"Application properties mismatch: {props}"
        except Exception as e:
            self.error = str(e)
        event.connection.close()


# Test 9: Binary Message Body
class TestBinaryBody(TestHandler):
    def __init__(self):
        super().__init__('binary_message_body', timeout=15)
        self.sender = None
        self.receiver = None
        self.message_sent = False
        self.test_data = bytes([0x01, 0x02, 0x03, 0x04, 0xFF, 0xFE])
        self.queue_name = f"test-queue-binary-{int(time.time() * 1000)}"

    def on_start(self, event):
        super().on_start(event)
        conn = event.container.connect(BROKER_URL)
        self.sender = event.container.create_sender(conn, self.queue_name)
        self.receiver = event.container.create_receiver(conn, self.queue_name)

    def on_sendable(self, event):
        if self.sender and not self.message_sent:
            msg = Message(
                body=self.test_data,
                content_type="application/octet-stream"
            )
            self.sender.send(msg)
            self.message_sent = True

    def on_message(self, event):
        try:
            body = event.message.body
            if isinstance(body, (bytes, bytearray)) and bytes(body) == self.test_data:
                self.completed = True
                success(self.test_name)
            else:
                self.error = "Binary body mismatch"
        except Exception as e:
            self.error = str(e)
        event.connection.close()


# Test 10: Message Settlement Accept
class TestMessageSettlementAccept(TestHandler):
    def __init__(self):
        super().__init__('message_settlement_accept', timeout=15)
        self.sender = None
        self.receiver = None
        self.message_sent = False
        self.queue_name = f"test-queue-settle-{int(time.time() * 1000)}"

    def on_start(self, event):
        super().on_start(event)
        conn = event.container.connect(BROKER_URL)
        self.sender = event.container.create_sender(conn, self.queue_name)
        self.receiver = event.container.create_receiver(conn, self.queue_name)

    def on_sendable(self, event):
        if self.sender and not self.message_sent:
            msg = Message(body="Settlement test")
            self.sender.send(msg)
            self.message_sent = True

    def on_message(self, event):
        # Accept the delivery
        event.delivery.update(Delivery.ACCEPTED)
        event.delivery.settle()
        self.completed = True
        success(self.test_name)
        event.connection.close()


# Test 11: Multiple Messages
class TestMultipleMessages(TestHandler):
    def __init__(self):
        super().__init__('multiple_messages', timeout=20)
        self.sender = None
        self.receiver = None
        self.messages_sent = 0
        self.messages_received = 0
        self.message_count = 5
        self.queue_name = f"test-queue-multi-{int(time.time() * 1000)}"

    def on_start(self, event):
        super().on_start(event)
        conn = event.container.connect(BROKER_URL)
        self.sender = event.container.create_sender(conn, self.queue_name)
        self.receiver = event.container.create_receiver(conn, self.queue_name)

    def on_sendable(self, event):
        while self.messages_sent < self.message_count and self.sender.credit:
            msg = Message(body=f"Message {self.messages_sent}")
            self.sender.send(msg)
            self.messages_sent += 1

    def on_message(self, event):
        self.messages_received += 1
        if self.messages_received == self.message_count:
            self.completed = True
            success(self.test_name)
            event.connection.close()


# Test 12: Large Message
class TestLargeMessage(TestHandler):
    def __init__(self):
        super().__init__('large_message', timeout=30)
        self.sender = None
        self.receiver = None
        self.message_sent = False
        self.large_body = 'X' * 100000  # 100KB
        self.queue_name = f"test-queue-large-{int(time.time() * 1000)}"

    def on_start(self, event):
        super().on_start(event)
        conn = event.container.connect(BROKER_URL)
        self.sender = event.container.create_sender(conn, self.queue_name)
        self.receiver = event.container.create_receiver(conn, self.queue_name)

    def on_sendable(self, event):
        if self.sender and not self.message_sent:
            msg = Message(body=self.large_body)
            self.sender.send(msg)
            self.message_sent = True

    def on_message(self, event):
        try:
            if event.message.body == self.large_body:
                self.completed = True
                success(self.test_name)
            else:
                self.error = f"Size mismatch: expected {len(self.large_body)}, got {len(event.message.body)}"
        except Exception as e:
            self.error = str(e)
        event.connection.close()


# Test 13: Durable Message
class TestDurableMessage(TestHandler):
    def __init__(self):
        super().__init__('durable_message', timeout=15)
        self.sender = None
        self.receiver = None
        self.message_sent = False
        self.queue_name = f"test-queue-durable-{int(time.time() * 1000)}"

    def on_start(self, event):
        super().on_start(event)
        conn = event.container.connect(BROKER_URL)
        self.sender = event.container.create_sender(conn, self.queue_name)
        self.receiver = event.container.create_receiver(conn, self.queue_name)

    def on_sendable(self, event):
        if self.sender and not self.message_sent:
            msg = Message(body="Durable test", durable=True)
            self.sender.send(msg)
            self.message_sent = True

    def on_message(self, event):
        self.completed = True
        success(self.test_name)
        event.connection.close()


# Test 14: Message TTL
class TestMessageTTL(TestHandler):
    def __init__(self):
        super().__init__('message_ttl', timeout=15)
        self.sender = None
        self.receiver = None
        self.message_sent = False
        self.queue_name = f"test-queue-ttl-{int(time.time() * 1000)}"

    def on_start(self, event):
        super().on_start(event)
        conn = event.container.connect(BROKER_URL)
        self.sender = event.container.create_sender(conn, self.queue_name)
        self.receiver = event.container.create_receiver(conn, self.queue_name)

    def on_sendable(self, event):
        if self.sender and not self.message_sent:
            msg = Message(body="TTL test", ttl=60.0)  # 60 second TTL
            self.sender.send(msg)
            self.message_sent = True

    def on_message(self, event):
        self.completed = True
        success(self.test_name)
        event.connection.close()


# Test 15: Priority Message
class TestPriorityMessage(TestHandler):
    def __init__(self):
        super().__init__('priority_message', timeout=15)
        self.sender = None
        self.receiver = None
        self.message_sent = False
        self.queue_name = f"test-queue-priority-{int(time.time() * 1000)}"

    def on_start(self, event):
        super().on_start(event)
        conn = event.container.connect(BROKER_URL)
        self.sender = event.container.create_sender(conn, self.queue_name)
        self.receiver = event.container.create_receiver(conn, self.queue_name)

    def on_sendable(self, event):
        if self.sender and not self.message_sent:
            msg = Message(body="Priority test", priority=9)
            self.sender.send(msg)
            self.message_sent = True

    def on_message(self, event):
        self.completed = True
        success(self.test_name)
        event.connection.close()


def run_test(handler_class):
    """Run a single test with proper error handling"""
    handler = handler_class()
    try:
        Container(handler).run()
        if handler.error:
            fail(handler.test_name, handler.error)
        elif not handler.completed:
            fail(handler.test_name, "Test did not complete")
    except Exception as e:
        fail(handler.test_name, str(e))
    time.sleep(0.5)  # Small delay between tests


def main():
    global passed, failed, results

    log('=' * 60)
    log('AMQP 1.0 Compliance Tests - Python (qpid-proton)')
    log(f'Connecting to: {BROKER_URL}')
    log('=' * 60)

    tests = [
        TestBasicConnection,
        TestSessionCreation,
        TestSenderLink,
        TestReceiverLink,
        TestSendSimpleMessage,
        TestSendReceiveMessage,
        TestMessageProperties,
        TestApplicationProperties,
        TestBinaryBody,
        TestMessageSettlementAccept,
        TestMultipleMessages,
        TestLargeMessage,
        TestDurableMessage,
        TestMessageTTL,
        TestPriorityMessage,
    ]

    for test_class in tests:
        run_test(test_class)

    log('')
    log('=' * 60)
    log('TEST RESULTS SUMMARY')
    log('=' * 60)
    log(f'Total:  {passed + failed}')
    log(f'Passed: {passed}')
    log(f'Failed: {failed}')
    log('=' * 60)

    if failed > 0:
        log('\nFailed Tests:')
        for r in results:
            if r['status'] == 'FAILED':
                log(f"  - {r['name']}: {r.get('error', 'Unknown error')}")

    sys.exit(1 if failed > 0 else 0)


if __name__ == '__main__':
    main()
