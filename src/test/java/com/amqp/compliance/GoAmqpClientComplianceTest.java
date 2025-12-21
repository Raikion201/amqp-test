package com.amqp.compliance;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP Compliance Tests using Go amqp091-go client.
 *
 * Uses the official RabbitMQ Go client: github.com/rabbitmq/amqp091-go
 * https://github.com/rabbitmq/amqp091-go
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Go amqp091-go Client Compliance Tests")
public class GoAmqpClientComplianceTest extends DockerClientTestBase {

    private static final Logger logger = LoggerFactory.getLogger(GoAmqpClientComplianceTest.class);

    // Go test script that uses amqp091-go
    private static final String GO_TEST_SCRIPT = """
            package main

            import (
                "fmt"
                "log"
                "os"
                "time"

                amqp "github.com/rabbitmq/amqp091-go"
            )

            func main() {
                url := os.Getenv("AMQP_URL")
                if url == "" {
                    url = "amqp://guest:guest@amqp-server:5672/"
                }

                fmt.Printf("Connecting to %s\\n", url)

                // Test 1: Connection
                conn, err := amqp.Dial(url)
                if err != nil {
                    log.Fatalf("FAIL: Connection failed: %v", err)
                }
                defer conn.Close()
                fmt.Println("PASS: Connection established")

                // Test 2: Channel
                ch, err := conn.Channel()
                if err != nil {
                    log.Fatalf("FAIL: Channel creation failed: %v", err)
                }
                defer ch.Close()
                fmt.Println("PASS: Channel created")

                // Test 3: Queue Declaration
                q, err := ch.QueueDeclare(
                    "go-test-queue",
                    false, // durable
                    true,  // auto-delete
                    false, // exclusive
                    false, // no-wait
                    nil,   // args
                )
                if err != nil {
                    log.Fatalf("FAIL: Queue declaration failed: %v", err)
                }
                fmt.Printf("PASS: Queue declared: %s\\n", q.Name)

                // Test 4: Publish
                body := "Hello from Go amqp091-go!"
                err = ch.Publish(
                    "",     // exchange
                    q.Name, // routing key
                    false,  // mandatory
                    false,  // immediate
                    amqp.Publishing{
                        ContentType: "text/plain",
                        Body:        []byte(body),
                    },
                )
                if err != nil {
                    log.Fatalf("FAIL: Publish failed: %v", err)
                }
                fmt.Println("PASS: Message published")

                // Test 5: Consume
                msgs, err := ch.Consume(
                    q.Name,
                    "",    // consumer
                    true,  // auto-ack
                    false, // exclusive
                    false, // no-local
                    false, // no-wait
                    nil,   // args
                )
                if err != nil {
                    log.Fatalf("FAIL: Consume failed: %v", err)
                }

                select {
                case msg := <-msgs:
                    if string(msg.Body) == body {
                        fmt.Printf("PASS: Message received: %s\\n", string(msg.Body))
                    } else {
                        log.Fatalf("FAIL: Message mismatch: got %s, want %s", string(msg.Body), body)
                    }
                case <-time.After(5 * time.Second):
                    log.Fatalf("FAIL: Timeout waiting for message")
                }

                // Test 6: Exchange Declaration
                err = ch.ExchangeDeclare(
                    "go-test-exchange",
                    "direct",
                    false, // durable
                    true,  // auto-delete
                    false, // internal
                    false, // no-wait
                    nil,   // args
                )
                if err != nil {
                    log.Fatalf("FAIL: Exchange declaration failed: %v", err)
                }
                fmt.Println("PASS: Exchange declared")

                // Test 7: Queue Bind
                err = ch.QueueBind(
                    q.Name,
                    "test-key",
                    "go-test-exchange",
                    false,
                    nil,
                )
                if err != nil {
                    log.Fatalf("FAIL: Queue bind failed: %v", err)
                }
                fmt.Println("PASS: Queue bound to exchange")

                // Test 8: QoS
                err = ch.Qos(10, 0, false)
                if err != nil {
                    log.Fatalf("FAIL: QoS failed: %v", err)
                }
                fmt.Println("PASS: QoS set")

                // Cleanup
                ch.QueueDelete(q.Name, false, false, false)
                ch.ExchangeDelete("go-test-exchange", false, false)

                fmt.Println("\\n=== All Go amqp091-go tests passed! ===")
            }
            """;

    @Test
    @Order(1)
    @DisplayName("Go: Run amqp091-go compliance tests")
    void testGoAmqpClient() throws Exception {
        logger.info("Running Go amqp091-go client tests...");

        // Create container that installs and runs Go AMQP client
        GenericContainer<?> goClient = new GenericContainer<>("golang:1.21-alpine")
                .withNetwork(network)
                .withEnv("AMQP_URL", getAmqpUrl())
                .withEnv("GOPATH", "/go")
                .withCommand("sh", "-c",
                        "mkdir -p /app && cd /app && " +
                        "go mod init amqp-test && " +
                        "go get github.com/rabbitmq/amqp091-go@v1.9.0 && " +
                        "cat > main.go << 'GOEOF'\n" + GO_TEST_SCRIPT + "\nGOEOF\n" +
                        "go run main.go")
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("GO-CLIENT"));

        String logs = runClientAndGetLogs(goClient);
        logger.info("Go client output:\n{}", logs);

        assertTrue(logs.contains("All Go amqp091-go tests passed!"),
                "Go amqp091-go tests should pass. Output: " + logs);
    }
}
