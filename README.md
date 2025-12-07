# AMCP Server

A Java/Netty AMQP server backend with PostgreSQL persistence, designed to function like RabbitMQ.

## Features

- **AMQP 0-9-1 Protocol Support**: Compatible with standard AMQP clients
- **Exchange Types**: Direct, Fanout, Topic, and Headers exchanges
- **Message Persistence**: PostgreSQL backend for durable messages and metadata
- **High Performance**: Built on Netty for efficient network I/O
- **Queue Management**: Durable, exclusive, and auto-delete queues
- **Message Routing**: Full topic pattern matching and direct routing
- **Connection Management**: Multi-channel support with proper connection handling

## Quick Start

### Prerequisites

- Java 17 or higher
- PostgreSQL 12 or higher
- Maven 3.6 or higher

### Database Setup

1. Create a PostgreSQL database:
```sql
CREATE DATABASE amcp;
CREATE USER amcp WITH PASSWORD 'amcp';
GRANT ALL PRIVILEGES ON DATABASE amcp TO amcp;
```

2. The server will automatically create the required tables on startup.

### Building

```bash
mvn clean package
```

### Running

```bash
java -jar target/amcp-server-1.0.0.jar
```

Or with custom configuration:

```bash
java -jar target/amcp-server-1.0.0.jar \
  --port 5673 \
  --db-url jdbc:postgresql://localhost:5432/amcp \
  --db-user amcp \
  --db-password amcp
```

### Configuration Options

- `--host HOST`: Server host (default: localhost)
- `--port PORT`: Server port (default: 5672)
- `--db-url URL`: PostgreSQL JDBC URL
- `--db-user USER`: Database username
- `--db-password PASS`: Database password
- `--help`: Show help message

## Architecture

### Core Components

- **AmqpServer**: Netty-based server handling AMQP connections
- **AmqpBroker**: Message broker managing exchanges, queues, and routing
- **AmqpConnection**: Connection handler managing multiple channels
- **AmqpChannel**: Channel implementation handling AMQP commands
- **PersistenceManager**: PostgreSQL integration for message persistence

### Exchange Types

1. **Direct Exchange**: Routes messages based on exact routing key match
2. **Fanout Exchange**: Routes messages to all bound queues
3. **Topic Exchange**: Routes messages using pattern matching (*, #)
4. **Headers Exchange**: Routes based on message headers (basic support)

### Message Flow

1. Client connects and opens channels
2. Client declares exchanges and queues
3. Client binds queues to exchanges with routing keys
4. Messages are published to exchanges
5. Exchanges route messages to appropriate queues
6. Consumers receive messages from queues

## Client Compatibility

The server is compatible with standard AMQP 0-9-1 clients including:

- RabbitMQ Java Client
- Pika (Python)
- amqplib (Node.js)
- bunny (Ruby)

## Example Usage

### Java Client Example

```java
ConnectionFactory factory = new ConnectionFactory();
factory.setHost("localhost");
factory.setPort(5672);

try (Connection connection = factory.newConnection();
     Channel channel = connection.createChannel()) {
    
    // Declare exchange and queue
    channel.exchangeDeclare("test_exchange", "direct", true);
    channel.queueDeclare("test_queue", true, false, false, null);
    channel.queueBind("test_queue", "test_exchange", "test_key");
    
    // Publish message
    String message = "Hello, AMCP!";
    channel.basicPublish("test_exchange", "test_key", null, message.getBytes());
    
    // Consume message
    channel.basicConsume("test_queue", true, (consumerTag, delivery) -> {
        String receivedMessage = new String(delivery.getBody());
        System.out.println("Received: " + receivedMessage);
    }, consumerTag -> {});
}
```

## Database Schema

The server automatically creates the following tables:

- `exchanges`: Exchange definitions
- `queues`: Queue definitions  
- `bindings`: Queue-to-exchange bindings
- `messages`: Persistent message storage

## Logging

Logs are written to both console and `logs/amcp-server.log` with automatic rotation.

## Performance Considerations

- Connection pooling is configured for optimal database performance
- Message persistence only occurs for durable queues and persistent messages
- Netty provides efficient network I/O handling
- Topic pattern matching is optimized for common use cases

## Limitations

- Headers exchange routing is not fully implemented
- Some advanced AMQP features may not be supported
- No clustering or high availability features
- No management UI (planned for future versions)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see LICENSE file for details.