# AMCP Server - Unit Tests

## Overview

This directory contains comprehensive unit tests for the AMCP (Advanced Message Control Protocol) server, designed to test RabbitMQ-like functionality.

## Test Structure

### Model Tests
- **QueueTest.java** - Tests queue operations (enqueue, dequeue, FIFO, purge, concurrent operations)
- **MessageTest.java** - Tests message properties, headers, delivery modes, and persistence flags
- **ExchangeTest.java** - Tests all exchange types:
  - Direct exchange with exact routing key matching
  - Fanout exchange broadcasting to all queues
  - Topic exchange with wildcard routing (`*` and `#`)
  - Headers exchange (basic structure)

### Server Tests
- **AmqpBrokerTest.java** - Core broker functionality:
  - Default exchanges initialization
  - Exchange declaration and management
  - Queue declaration and management
  - Queue bindings
  - Message publishing with various routing strategies
  - Message consumption
  - Persistence operations

### Connection Tests
- **AmqpConnectionTest.java** - AMQP connection management:
  - Connection lifecycle
  - Multiple channels per connection
  - Frame sending and receiving
  - Virtual host and authentication
  - Channel multiplexing

- **AmqpChannelTest.java** - Channel operations:
  - Channel lifecycle (open/close)
  - Consumer management
  - Message acknowledgements
  - QoS (Quality of Service) settings
  - Transaction support (tx.select, tx.commit, tx.rollback)

### Integration Tests
- **AmqpIntegrationTest.java** - Real-world RabbitMQ patterns:
  - **Work Queue Pattern** - Multiple workers consuming from a single queue
  - **Pub/Sub Pattern** - Fanout exchange broadcasting to multiple subscribers
  - **Routing Pattern** - Direct exchange with multiple routing keys
  - **Topic Pattern** - Complex topic-based routing with wildcards
  - **RPC Pattern** - Request/reply with correlation IDs
  - **Dead Letter Queue** - Failed message handling
  - **Priority Queue** - Message prioritization structure
  - **Message TTL** - Time-to-live simulation
  - **High Throughput** - 10,000 messages with concurrent publishers/consumers

### Persistence Tests
- **PersistenceManagerTest.java** - Database persistence:
  - Durable exchange persistence
  - Durable queue persistence
  - Message persistence
  - Binding persistence
  - Error handling

## Running the Tests

### Run All Tests
```powershell
mvn test
```

### Run Specific Test Class
```powershell
mvn test -Dtest=QueueTest
mvn test -Dtest=AmqpBrokerTest
mvn test -Dtest=AmqpIntegrationTest
```

### Run Tests with Specific Pattern
```powershell
mvn test -Dtest=*Test
mvn test -Dtest=*IntegrationTest
```

### Run with Coverage
```powershell
mvn clean test jacoco:report
```

### Run Specific Test Method
```powershell
mvn test -Dtest=ExchangeTest#testTopicRoutingHashWildcard
```

## Test Coverage

The test suite covers:

✅ **Queue Management**
- Queue declaration (durable, exclusive, auto-delete)
- FIFO message ordering
- Concurrent enqueue/dequeue operations
- Queue purging
- Automatic queue name generation

✅ **Exchange Types**
- Direct: Exact routing key matching
- Fanout: Broadcast to all bound queues
- Topic: Wildcard routing with `*` (one word) and `#` (zero or more words)
- Headers: Basic structure

✅ **Message Properties**
- Body, routing key, content type, encoding
- Delivery mode (persistent/non-persistent)
- Priority, correlation ID, reply-to
- Message headers, expiration, timestamps

✅ **Routing & Bindings**
- Queue-to-exchange bindings
- Multiple queues per routing key
- Multiple routing keys per queue
- Complex topic patterns

✅ **Persistence**
- Durable exchanges
- Durable queues
- Persistent messages
- Binding persistence

✅ **Connection & Channel Management**
- Multiple channels per connection
- Channel lifecycle
- Consumer management
- Flow control (QoS)
- Transactions

✅ **RabbitMQ Patterns**
- Work queues
- Publish/Subscribe
- Routing
- Topics
- RPC
- Dead letter queues
- Priority queues

## Test Statistics

- **Total Test Classes**: 9
- **Total Test Methods**: 100+
- **Integration Scenarios**: 10
- **Concurrency Tests**: 3

## Dependencies

The tests use:
- **JUnit 5** - Testing framework
- **Mockito** - Mocking framework
- **AssertJ** - Fluent assertions

## Notes

1. **Mock Dependencies**: Most tests use mocked `PersistenceManager` to avoid database dependencies
2. **Thread Safety**: Includes concurrent testing for queues and broker operations
3. **Real Scenarios**: Integration tests simulate actual RabbitMQ usage patterns
4. **Error Handling**: Tests verify both happy paths and error conditions

## Next Steps

To enhance test coverage, consider adding:
- Network protocol tests (AMQP frame encoding/decoding)
- Security and authentication tests
- Performance benchmarks
- Cluster/HA scenarios
- More complex routing patterns
- Consumer acknowledgement strategies

## CI/CD Integration

These tests can be integrated into your CI/CD pipeline:

```yaml
# Example GitHub Actions
- name: Run Tests
  run: mvn test

- name: Generate Coverage Report
  run: mvn jacoco:report

- name: Upload Coverage
  uses: codecov/codecov-action@v3
```
