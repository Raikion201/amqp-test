package com.amqp.compliance;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP Compliance Tests using Elixir amqp client.
 *
 * Uses the popular Elixir AMQP client: amqp
 * https://github.com/pma/amqp
 * https://hex.pm/packages/amqp
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Elixir AMQP Client Compliance Tests")
public class ElixirAmqpClientComplianceTest extends DockerClientTestBase {

    private static final Logger logger = LoggerFactory.getLogger(ElixirAmqpClientComplianceTest.class);

    // Elixir test script using amqp
    private static final String ELIXIR_TEST_SCRIPT = """
Mix.install([{:amqp, "~> 3.3"}])

defmodule AmqpTest do
  def run do
    host = System.get_env("AMQP_HOST", "amqp-server")
    port = String.to_integer(System.get_env("AMQP_PORT", "5672"))

    IO.puts("Connecting to #{host}:#{port}")

    # Test 1: Connection
    {:ok, conn} = AMQP.Connection.open(
      host: host,
      port: port,
      username: "guest",
      password: "guest",
      virtual_host: "/"
    )
    IO.puts("PASS: Connection established")

    # Test 2: Channel
    {:ok, chan} = AMQP.Channel.open(conn)
    IO.puts("PASS: Channel created")

    # Test 3: Queue Declaration
    {:ok, %{queue: queue_name}} = AMQP.Queue.declare(chan, "elixir-test-queue", auto_delete: true)
    IO.puts("PASS: Queue declared: #{queue_name}")

    # Test 4: Publish
    message = "Hello from Elixir AMQP!"
    :ok = AMQP.Basic.publish(chan, "", queue_name, message, content_type: "text/plain")
    IO.puts("PASS: Message published")

    # Wait for message
    :timer.sleep(500)

    # Test 5: Consume (basic get)
    case AMQP.Basic.get(chan, queue_name, no_ack: true) do
      {:ok, ^message, _meta} ->
        IO.puts("PASS: Message received: #{message}")
      {:ok, received, _meta} ->
        IO.puts("FAIL: Message mismatch: #{received}")
        System.halt(1)
      {:empty, _} ->
        IO.puts("FAIL: No message received")
        System.halt(1)
    end

    # Test 6: Exchange Declaration
    :ok = AMQP.Exchange.declare(chan, "elixir-test-exchange", :direct, auto_delete: true)
    IO.puts("PASS: Direct exchange declared")

    # Test 7: Queue Bind
    :ok = AMQP.Queue.bind(chan, queue_name, "elixir-test-exchange", routing_key: "test-key")
    IO.puts("PASS: Queue bound to exchange")

    # Test 8: Publish to exchange
    :ok = AMQP.Basic.publish(chan, "elixir-test-exchange", "test-key", "Message via exchange")
    IO.puts("PASS: Message published to exchange")

    :timer.sleep(500)

    # Test 9: Get from queue
    case AMQP.Basic.get(chan, queue_name, no_ack: true) do
      {:ok, received, _meta} ->
        IO.puts("PASS: Message received from exchange: #{received}")
      _ -> :ok
    end

    # Test 10: QoS
    :ok = AMQP.Basic.qos(chan, prefetch_count: 10)
    IO.puts("PASS: QoS set")

    # Test 11: Topic exchange
    :ok = AMQP.Exchange.declare(chan, "elixir-topic-exchange", :topic, auto_delete: true)
    IO.puts("PASS: Topic exchange declared")

    # Test 12: Fanout exchange
    :ok = AMQP.Exchange.declare(chan, "elixir-fanout-exchange", :fanout, auto_delete: true)
    IO.puts("PASS: Fanout exchange declared")

    # Test 13: Headers exchange
    :ok = AMQP.Exchange.declare(chan, "elixir-headers-exchange", :headers, auto_delete: true)
    IO.puts("PASS: Headers exchange declared")

    # Test 14: Multiple channels
    channels = for _ <- 1..3 do
      {:ok, ch} = AMQP.Channel.open(conn)
      ch
    end
    IO.puts("PASS: Multiple channels created")
    Enum.each(channels, &AMQP.Channel.close/1)

    # Cleanup
    AMQP.Queue.delete(chan, queue_name)
    AMQP.Exchange.delete(chan, "elixir-test-exchange")
    AMQP.Exchange.delete(chan, "elixir-topic-exchange")
    AMQP.Exchange.delete(chan, "elixir-fanout-exchange")
    AMQP.Exchange.delete(chan, "elixir-headers-exchange")

    AMQP.Channel.close(chan)
    AMQP.Connection.close(conn)

    IO.puts("\\n=== All Elixir AMQP tests passed! ===")
  end
end

AmqpTest.run()
""";

    @Test
    @Order(1)
    @DisplayName("Elixir: Run AMQP compliance tests")
    void testElixirAmqpClient() throws Exception {
        logger.info("Running Elixir AMQP client tests...");

        GenericContainer<?> elixirClient = new GenericContainer<>("elixir:1.15-slim")
                .withNetwork(network)
                .withEnv("AMQP_HOST", getAmqpHost())
                .withEnv("AMQP_PORT", String.valueOf(AMQP_PORT))
                .withEnv("MIX_ENV", "prod")
                .withCommand("sh", "-c",
                        "cat > test.exs << 'EXSEOF'\n" + ELIXIR_TEST_SCRIPT + "\nEXSEOF\n" +
                        "elixir test.exs")
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("ELIXIR-CLIENT"));

        String logs = runClientAndGetLogs(elixirClient);
        logger.info("Elixir client output:\n{}", logs);

        assertTrue(logs.contains("All Elixir AMQP tests passed!"),
                "Elixir AMQP tests should pass. Output: " + logs);
    }
}
