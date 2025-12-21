package com.amqp.compliance;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP Compliance Tests using PHP php-amqplib client.
 *
 * Uses the popular PHP AMQP client: php-amqplib
 * https://github.com/php-amqplib/php-amqplib
 * https://packagist.org/packages/php-amqplib/php-amqplib
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("PHP php-amqplib Client Compliance Tests")
public class PhpAmqplibClientComplianceTest extends DockerClientTestBase {

    private static final Logger logger = LoggerFactory.getLogger(PhpAmqplibClientComplianceTest.class);

    // PHP test script using php-amqplib
    private static final String PHP_TEST_SCRIPT = """
<?php
require_once __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\\Connection\\AMQPStreamConnection;
use PhpAmqpLib\\Message\\AMQPMessage;
use PhpAmqpLib\\Exchange\\AMQPExchangeType;

$host = getenv('AMQP_HOST') ?: 'amqp-server';
$port = getenv('AMQP_PORT') ?: 5672;

echo "Connecting to $host:$port\\n";

try {
    // Test 1: Connection (using PLAIN auth instead of AMQPLAIN)
    $connection = new AMQPStreamConnection(
        $host, $port, 'guest', 'guest', '/',
        false,  // insist
        'PLAIN' // login_method - use PLAIN instead of AMQPLAIN
    );
    echo "PASS: Connection established\\n";

    // Test 2: Channel
    $channel = $connection->channel();
    echo "PASS: Channel created\\n";

    // Test 3: Queue Declaration
    list($queueName, ,) = $channel->queue_declare('php-test-queue', false, false, false, true);
    echo "PASS: Queue declared: $queueName\\n";

    // Test 4: Publish
    $messageBody = 'Hello from PHP php-amqplib!';
    $msg = new AMQPMessage($messageBody, ['content_type' => 'text/plain']);
    $channel->basic_publish($msg, '', $queueName);
    echo "PASS: Message published\\n";

    // Test 5: Consume (basic_get)
    $message = $channel->basic_get($queueName, true);
    if ($message && $message->body === $messageBody) {
        echo "PASS: Message received: " . $message->body . "\\n";
    } else {
        echo "FAIL: Message mismatch or not received\\n";
        exit(1);
    }

    // Test 6: Exchange Declaration
    $channel->exchange_declare('php-test-exchange', AMQPExchangeType::DIRECT, false, false, true);
    echo "PASS: Direct exchange declared\\n";

    // Test 7: Queue Bind
    $channel->queue_bind($queueName, 'php-test-exchange', 'test-key');
    echo "PASS: Queue bound to exchange\\n";

    // Test 8: Publish to exchange
    $msg2 = new AMQPMessage('Message via exchange');
    $channel->basic_publish($msg2, 'php-test-exchange', 'test-key');
    echo "PASS: Message published to exchange\\n";

    usleep(500000); // 500ms

    // Test 9: Get from queue
    $message = $channel->basic_get($queueName, true);
    if ($message) {
        echo "PASS: Message received from exchange: " . $message->body . "\\n";
    }

    // Test 10: QoS
    $channel->basic_qos(0, 10, false);
    echo "PASS: QoS set\\n";

    // Test 11: Topic exchange
    $channel->exchange_declare('php-topic-exchange', AMQPExchangeType::TOPIC, false, false, true);
    echo "PASS: Topic exchange declared\\n";

    // Test 12: Fanout exchange
    $channel->exchange_declare('php-fanout-exchange', AMQPExchangeType::FANOUT, false, false, true);
    echo "PASS: Fanout exchange declared\\n";

    // Test 13: Headers exchange
    $channel->exchange_declare('php-headers-exchange', AMQPExchangeType::HEADERS, false, false, true);
    echo "PASS: Headers exchange declared\\n";

    // Test 14: Transaction
    $channel->tx_select();
    echo "PASS: Transaction mode enabled\\n";
    $channel->tx_commit();
    echo "PASS: Transaction committed\\n";

    // Test 15: Multiple queues
    for ($i = 0; $i < 3; $i++) {
        $channel->queue_declare("php-multi-queue-$i", false, false, false, true);
    }
    echo "PASS: Multiple queues declared\\n";

    // Cleanup
    $channel->queue_delete($queueName);
    $channel->exchange_delete('php-test-exchange');
    $channel->exchange_delete('php-topic-exchange');
    $channel->exchange_delete('php-fanout-exchange');
    $channel->exchange_delete('php-headers-exchange');
    for ($i = 0; $i < 3; $i++) {
        $channel->queue_delete("php-multi-queue-$i");
    }

    $channel->close();
    $connection->close();

    echo "\\n=== All PHP php-amqplib tests passed! ===\\n";

} catch (Exception $e) {
    echo "FAIL: " . $e->getMessage() . "\\n";
    exit(1);
}
""";

    @Test
    @Order(1)
    @DisplayName("PHP: Run php-amqplib compliance tests")
    void testPhpAmqplibClient() throws Exception {
        logger.info("Running PHP php-amqplib client tests...");

        // Use php:8.2-cli and install bcmath extension + composer
        GenericContainer<?> phpClient = new GenericContainer<>("php:8.2-cli")
                .withNetwork(network)
                .withEnv("AMQP_HOST", getAmqpHost())
                .withEnv("AMQP_PORT", String.valueOf(AMQP_PORT))
                .withCommand("sh", "-c",
                        "apt-get update -qq && apt-get install -qq -y unzip libonig-dev > /dev/null 2>&1 && " +
                        "docker-php-ext-install bcmath sockets > /dev/null 2>&1 && " +
                        "curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer > /dev/null 2>&1 && " +
                        "mkdir -p /app && cd /app && " +
                        "composer require php-amqplib/php-amqplib --quiet 2>/dev/null && " +
                        "cat > test.php << 'PHPEOF'\n" + PHP_TEST_SCRIPT + "\nPHPEOF\n" +
                        "php test.php")
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("PHP-CLIENT"));

        String logs = runClientAndGetLogs(phpClient);
        logger.info("PHP client output:\n{}", logs);

        assertTrue(logs.contains("All PHP php-amqplib tests passed!"),
                "PHP php-amqplib tests should pass. Output: " + logs);
    }
}
