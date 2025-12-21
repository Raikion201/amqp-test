package com.amqp.compliance;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP Compliance Tests using Ruby Bunny client.
 *
 * Uses the popular Ruby AMQP client: bunny
 * https://github.com/ruby-amqp/bunny
 * https://rubygems.org/gems/bunny
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Ruby Bunny Client Compliance Tests")
public class RubyBunnyClientComplianceTest extends DockerClientTestBase {

    private static final Logger logger = LoggerFactory.getLogger(RubyBunnyClientComplianceTest.class);

    // Ruby test script using bunny
    private static final String RUBY_TEST_SCRIPT = """
require 'bunny'

amqp_host = ENV['AMQP_HOST'] || 'amqp-server'
amqp_port = (ENV['AMQP_PORT'] || '5672').to_i

puts "Connecting to #{amqp_host}:#{amqp_port}"

begin
  # Test 1: Connection
  connection = Bunny.new(
    host: amqp_host,
    port: amqp_port,
    user: 'guest',
    password: 'guest',
    vhost: '/',
    automatically_recover: false
  )
  connection.start
  puts "PASS: Connection established"

  # Test 2: Channel
  channel = connection.create_channel
  puts "PASS: Channel created"

  # Test 3: Queue Declaration
  queue = channel.queue('ruby-test-queue', auto_delete: true)
  puts "PASS: Queue declared: #{queue.name}"

  # Test 4: Publish
  message = "Hello from Ruby Bunny!"
  queue.publish(message, content_type: 'text/plain')
  puts "PASS: Message published"

  # Wait for message
  sleep 0.5

  # Test 5: Consume
  delivery_info, properties, payload = queue.pop
  if payload == message
    puts "PASS: Message received: #{payload}"
  else
    puts "FAIL: Message mismatch or not received"
    exit 1
  end

  # Test 6: Exchange Declaration
  exchange = channel.direct('ruby-test-exchange', auto_delete: true)
  puts "PASS: Direct exchange declared"

  # Test 7: Queue Bind
  queue.bind(exchange, routing_key: 'test-key')
  puts "PASS: Queue bound to exchange"

  # Test 8: Publish to exchange
  exchange.publish("Message via exchange", routing_key: 'test-key')
  puts "PASS: Message published to exchange"

  sleep 0.5

  # Test 9: Get from queue
  delivery_info, properties, payload = queue.pop
  if payload
    puts "PASS: Message received from exchange: #{payload}"
  end

  # Test 10: Prefetch
  channel.prefetch(10)
  puts "PASS: Prefetch set"

  # Test 11: Topic exchange
  topic_exchange = channel.topic('ruby-topic-exchange', auto_delete: true)
  puts "PASS: Topic exchange declared"

  # Test 12: Fanout exchange
  fanout_exchange = channel.fanout('ruby-fanout-exchange', auto_delete: true)
  puts "PASS: Fanout exchange declared"

  # Test 13: Headers exchange
  headers_exchange = channel.headers('ruby-headers-exchange', auto_delete: true)
  puts "PASS: Headers exchange declared"

  # Test 14: Multiple channels
  channels = 3.times.map { connection.create_channel }
  puts "PASS: Multiple channels created"
  channels.each(&:close)

  # Test 15: Transaction
  tx_channel = connection.create_channel
  tx_channel.tx_select
  puts "PASS: Transaction mode enabled"
  tx_channel.tx_commit
  puts "PASS: Transaction committed"
  tx_channel.close

  # Cleanup
  queue.delete
  exchange.delete
  topic_exchange.delete
  fanout_exchange.delete
  headers_exchange.delete

  channel.close
  connection.close

  puts "\\n=== All Ruby Bunny tests passed! ==="

rescue => e
  puts "FAIL: #{e.message}"
  exit 1
end
""";

    @Test
    @Order(1)
    @DisplayName("Ruby: Run Bunny compliance tests")
    void testRubyBunnyClient() throws Exception {
        logger.info("Running Ruby Bunny client tests...");

        GenericContainer<?> rubyClient = new GenericContainer<>("ruby:3.2-slim")
                .withNetwork(network)
                .withEnv("AMQP_HOST", getAmqpHost())
                .withEnv("AMQP_PORT", String.valueOf(AMQP_PORT))
                .withCommand("sh", "-c",
                        "gem install bunny --quiet && " +
                        "ruby -e '" + RUBY_TEST_SCRIPT.replace("'", "'\"'\"'") + "'")
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("RUBY-CLIENT"));

        String logs = runClientAndGetLogs(rubyClient);
        logger.info("Ruby client output:\n{}", logs);

        assertTrue(logs.contains("All Ruby Bunny tests passed!"),
                "Ruby Bunny tests should pass. Output: " + logs);
    }
}
