package com.amqp.compliance;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP Compliance Tests using Python Pika client.
 *
 * Uses the popular Python AMQP client: pika
 * https://github.com/pika/pika
 * https://pypi.org/project/pika/
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Python Pika Client Compliance Tests")
public class PythonPikaClientComplianceTest extends DockerClientTestBase {

    private static final Logger logger = LoggerFactory.getLogger(PythonPikaClientComplianceTest.class);

    // Python test script using pika
    private static final String PYTHON_TEST_SCRIPT = """
import pika
import sys
import os

amqp_host = os.environ.get('AMQP_HOST', 'amqp-server')
amqp_port = int(os.environ.get('AMQP_PORT', '5672'))

print(f"Connecting to {amqp_host}:{amqp_port}")

try:
    # Test 1: Connection
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters(
        host=amqp_host,
        port=amqp_port,
        virtual_host='/',
        credentials=credentials,
        connection_attempts=3,
        retry_delay=2
    )
    connection = pika.BlockingConnection(parameters)
    print("PASS: Connection established")

    # Test 2: Channel
    channel = connection.channel()
    print("PASS: Channel created")

    # Test 3: Queue Declaration
    result = channel.queue_declare(queue='python-test-queue', auto_delete=True)
    queue_name = result.method.queue
    print(f"PASS: Queue declared: {queue_name}")

    # Test 4: Publish
    message = "Hello from Python Pika!"
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message,
        properties=pika.BasicProperties(
            content_type='text/plain',
            delivery_mode=1
        )
    )
    print("PASS: Message published")

    # Test 5: Consume (with get)
    method, properties, body = channel.basic_get(queue=queue_name, auto_ack=True)
    if body and body.decode() == message:
        print(f"PASS: Message received: {body.decode()}")
    else:
        print(f"FAIL: Message mismatch or not received")
        sys.exit(1)

    # Test 6: Exchange Declaration
    channel.exchange_declare(
        exchange='python-test-exchange',
        exchange_type='direct',
        auto_delete=True
    )
    print("PASS: Exchange declared")

    # Test 7: Queue Bind
    channel.queue_bind(
        queue=queue_name,
        exchange='python-test-exchange',
        routing_key='test-key'
    )
    print("PASS: Queue bound to exchange")

    # Test 8: Publish to exchange
    channel.basic_publish(
        exchange='python-test-exchange',
        routing_key='test-key',
        body="Message via exchange"
    )
    print("PASS: Message published to exchange")

    # Test 9: Consume from bound queue
    method, properties, body = channel.basic_get(queue=queue_name, auto_ack=True)
    if body:
        print(f"PASS: Message received from exchange: {body.decode()}")
    else:
        print("FAIL: No message received from exchange")
        sys.exit(1)

    # Test 10: QoS
    channel.basic_qos(prefetch_count=10)
    print("PASS: QoS set")

    # Test 11: Multiple queues
    for i in range(3):
        channel.queue_declare(queue=f'python-multi-queue-{i}', auto_delete=True)
    print("PASS: Multiple queues declared")

    # Test 12: Fanout exchange
    channel.exchange_declare(
        exchange='python-fanout-exchange',
        exchange_type='fanout',
        auto_delete=True
    )
    print("PASS: Fanout exchange declared")

    # Cleanup
    channel.queue_delete(queue=queue_name)
    channel.exchange_delete(exchange='python-test-exchange')
    channel.exchange_delete(exchange='python-fanout-exchange')
    for i in range(3):
        channel.queue_delete(queue=f'python-multi-queue-{i}')

    connection.close()
    print("\\n=== All Python Pika tests passed! ===")

except Exception as e:
    print(f"FAIL: {str(e)}")
    sys.exit(1)
""";

    @Test
    @Order(1)
    @DisplayName("Python: Run Pika compliance tests")
    void testPythonPikaClient() throws Exception {
        logger.info("Running Python Pika client tests...");

        GenericContainer<?> pythonClient = new GenericContainer<>("python:3.11-slim")
                .withNetwork(network)
                .withEnv("AMQP_HOST", getAmqpHost())
                .withEnv("AMQP_PORT", String.valueOf(AMQP_PORT))
                .withCommand("sh", "-c",
                        "pip install --quiet pika && " +
                        "python3 -c '" + PYTHON_TEST_SCRIPT.replace("'", "'\"'\"'") + "'")
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("PYTHON-CLIENT"));

        String logs = runClientAndGetLogs(pythonClient);
        logger.info("Python client output:\n{}", logs);

        assertTrue(logs.contains("All Python Pika tests passed!"),
                "Python Pika tests should pass. Output: " + logs);
    }
}
