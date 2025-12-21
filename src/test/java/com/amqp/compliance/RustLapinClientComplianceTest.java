package com.amqp.compliance;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP Compliance Tests using Rust lapin client.
 *
 * Uses the popular Rust AMQP client: lapin
 * https://github.com/amqp-rs/lapin
 * https://crates.io/crates/lapin
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Rust lapin Client Compliance Tests")
public class RustLapinClientComplianceTest extends DockerClientTestBase {

    private static final Logger logger = LoggerFactory.getLogger(RustLapinClientComplianceTest.class);

    // Rust test script using lapin
    private static final String RUST_MAIN = """
use lapin::{
    options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties,
    ExchangeKind,
};
use futures_lite::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = std::env::var("AMQP_HOST").unwrap_or_else(|_| "amqp-server".to_string());
    let port = std::env::var("AMQP_PORT").unwrap_or_else(|_| "5672".to_string());
    let addr = format!("amqp://guest:guest@{}:{}/", host, port);

    println!("Connecting to {}", addr);

    // Test 1: Connection
    let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;
    println!("PASS: Connection established");

    // Test 2: Channel
    let channel = conn.create_channel().await?;
    println!("PASS: Channel created");

    // Test 3: Queue Declaration
    let queue = channel
        .queue_declare(
            "rust-test-queue",
            QueueDeclareOptions {
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    println!("PASS: Queue declared: {}", queue.name());

    // Test 4: Publish
    let message = b"Hello from Rust lapin!";
    channel
        .basic_publish(
            "",
            queue.name().as_str(),
            BasicPublishOptions::default(),
            message,
            BasicProperties::default().with_content_type("text/plain".into()),
        )
        .await?;
    println!("PASS: Message published");

    // Test 5: Consume (using Stream API)
    let mut consumer = channel
        .basic_consume(
            queue.name().as_str(),
            "rust-consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    if let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        let received = String::from_utf8_lossy(&delivery.data);
        if received == "Hello from Rust lapin!" {
            println!("PASS: Message received: {}", received);
        }
        delivery.ack(BasicAckOptions::default()).await?;
    }

    // Test 6: Exchange Declaration
    channel
        .exchange_declare(
            "rust-test-exchange",
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    println!("PASS: Direct exchange declared");

    // Test 7: Queue Bind
    channel
        .queue_bind(
            queue.name().as_str(),
            "rust-test-exchange",
            "test-key",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    println!("PASS: Queue bound to exchange");

    // Test 8: QoS
    channel.basic_qos(10, BasicQosOptions::default()).await?;
    println!("PASS: QoS set");

    // Test 9: Topic exchange
    channel
        .exchange_declare(
            "rust-topic-exchange",
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    println!("PASS: Topic exchange declared");

    // Test 10: Fanout exchange
    channel
        .exchange_declare(
            "rust-fanout-exchange",
            ExchangeKind::Fanout,
            ExchangeDeclareOptions {
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    println!("PASS: Fanout exchange declared");

    // Cleanup
    channel
        .queue_delete(queue.name().as_str(), QueueDeleteOptions::default())
        .await?;
    channel
        .exchange_delete("rust-test-exchange", ExchangeDeleteOptions::default())
        .await?;
    channel
        .exchange_delete("rust-topic-exchange", ExchangeDeleteOptions::default())
        .await?;
    channel
        .exchange_delete("rust-fanout-exchange", ExchangeDeleteOptions::default())
        .await?;

    conn.close(0, "").await?;

    println!("\\n=== All Rust lapin tests passed! ===");
    Ok(())
}
""";

    private static final String CARGO_TOML = """
[package]
name = "amqp-test"
version = "0.1.0"
edition = "2021"

[dependencies]
lapin = "2.5"
tokio = { version = "1", features = ["full"] }
futures-lite = "2.0"
""";

    @Test
    @Order(1)
    @DisplayName("Rust: Run lapin compliance tests")
    void testRustLapinClient() throws Exception {
        logger.info("Running Rust lapin client tests...");

        // Rust needs pkg-config and OpenSSL for lapin (use latest Rust for 2024 edition support)
        GenericContainer<?> rustClient = new GenericContainer<>("rust:latest")
                .withNetwork(network)
                .withEnv("AMQP_HOST", getAmqpHost())
                .withEnv("AMQP_PORT", String.valueOf(AMQP_PORT))
                .withCommand("sh", "-c",
                        "apt-get update -qq && apt-get install -qq -y pkg-config libssl-dev > /dev/null 2>&1 && " +
                        "mkdir -p /app/src && cd /app && " +
                        "cat > Cargo.toml << 'CARGOEOF'\n" + CARGO_TOML + "\nCARGOEOF\n" +
                        "cat > src/main.rs << 'RUSTEOF'\n" + RUST_MAIN + "\nRUSTEOF\n" +
                        "echo 'Building Rust project (this may take a while)...' && " +
                        "cargo build --release 2>&1 && " +
                        "./target/release/amqp-test")
                .withLogConsumer(new Slf4jLogConsumer(logger).withPrefix("RUST-CLIENT"));

        String logs = runClientAndGetLogs(rustClient);
        logger.info("Rust client output:\n{}", logs);

        assertTrue(logs.contains("All Rust lapin tests passed!"),
                "Rust lapin tests should pass. Output: " + logs);
    }
}
