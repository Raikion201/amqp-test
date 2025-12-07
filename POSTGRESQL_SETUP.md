# PostgreSQL Setup and Testing Guide

## Overview

This AMQP server implementation uses PostgreSQL for persistent storage of exchanges, queues, bindings, and messages. This guide explains how to set up PostgreSQL for both development and testing.

## Production Setup

### 1. Install PostgreSQL

Download and install PostgreSQL 16 from: https://www.postgresql.org/download/

### 2. Create Database and User

```sql
CREATE DATABASE amcp;
CREATE USER amcp_user WITH PASSWORD 'amcp_password';
GRANT ALL PRIVILEGES ON DATABASE amcp TO amcp_user;
```

### 3. Configuration

Update `src/main/resources/application.properties` with your PostgreSQL connection details:

```properties
db.url=jdbc:postgresql://localhost:5432/amcp
db.username=amcp_user
db.password=amcp_password
```

### 4. Schema Initialization

The database schema will be automatically initialized when the application starts. The `DatabaseManager` class creates all required tables, indexes, and foreign key constraints.

## Testing with Testcontainers

### Prerequisites

**Docker Desktop must be installed and running** to run PostgreSQL integration tests.

- **Windows/Mac**: Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- **Linux**: Install Docker Engine

### Running PostgreSQL Integration Tests

By default, PostgreSQL integration tests are **disabled** to allow tests to run without Docker.

To enable PostgreSQL integration tests:

```bash
# Ensure Docker is running, then:
mvn test -Dpostgresql.tests.enabled=true
```

### Running All Tests Without PostgreSQL

```bash
# This will run all tests except PostgreSQL integration tests:
mvn test
```

### What the PostgreSQL Integration Tests Cover

The `PostgreSQLIntegrationTest` class provides comprehensive testing of:

1. **Database Connection** - Verifies Testcontainers can start PostgreSQL and establish connections
2. **Schema Validation** - Ensures all tables, indexes, and constraints are created correctly
3. **CRUD Operations** - Tests create, read, update, delete for all entities:
   - Exchanges (all 4 types: direct, fanout, topic, headers)
   - Queues (with durability, exclusivity, auto-delete flags)
   - Bindings (with routing keys and wildcards)
   - Messages (with all AMQP properties)
4. **Foreign Key Constraints** - Validates referential integrity
5. **Cascade Deletes** - Ensures proper cleanup of related entities
6. **Concurrency** - Tests thread-safe database operations (10 threads × 50 messages)
7. **Unique Constraints** - Validates duplicate prevention
8. **Persistence** - Simulates broker restart scenarios
9. **Index Performance** - Verifies indexes are created for optimal query performance

## Testing Without Docker (Using Mocks)

The project includes unit tests that use Mockito to test persistence logic without requiring a database:

```bash
mvn test
```

These tests (`PersistenceManagerTest`) validate:
- Save operations for all entity types
- Delete operations
- Error handling
- Database connection failures

## CI/CD Integration

For CI/CD pipelines, you have two options:

### Option 1: Use Testcontainers with Docker

Ensure Docker is available in your CI environment:

```yaml
# GitHub Actions example
services:
  docker:
    image: docker:20.10.7
    options: --privileged

steps:
  - name: Run tests with PostgreSQL
    run: mvn test -Dpostgresql.tests.enabled=true
```

### Option 2: Use External PostgreSQL

Set up a PostgreSQL service in your CI pipeline:

```yaml
services:
  postgres:
    image: postgres:16-alpine
    env:
      POSTGRES_DB: amcp_test
      POSTGRES_USER: test
      POSTGRES_PASSWORD: test
    ports:
      - 5432:5432
```

## Database Schema

The following tables are automatically created:

### Exchanges Table
```sql
CREATE TABLE exchanges (
    name VARCHAR(255) PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    durable BOOLEAN NOT NULL,
    auto_delete BOOLEAN NOT NULL,
    internal BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Queues Table
```sql
CREATE TABLE queues (
    name VARCHAR(255) PRIMARY KEY,
    durable BOOLEAN NOT NULL,
    exclusive BOOLEAN NOT NULL,
    auto_delete BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Bindings Table
```sql
CREATE TABLE bindings (
    id SERIAL PRIMARY KEY,
    exchange_name VARCHAR(255) NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    routing_key VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (exchange_name) REFERENCES exchanges(name) ON DELETE CASCADE,
    FOREIGN KEY (queue_name) REFERENCES queues(name) ON DELETE CASCADE,
    UNIQUE(exchange_name, queue_name, routing_key)
);
```

### Messages Table
```sql
CREATE TABLE messages (
    id BIGSERIAL PRIMARY KEY,
    queue_name VARCHAR(255) NOT NULL,
    routing_key VARCHAR(255),
    content_type VARCHAR(100),
    content_encoding VARCHAR(100),
    headers TEXT,
    delivery_mode SMALLINT,
    priority SMALLINT,
    correlation_id VARCHAR(255),
    reply_to VARCHAR(255),
    expiration VARCHAR(255),
    message_id VARCHAR(255),
    timestamp BIGINT,
    type VARCHAR(255),
    user_id VARCHAR(255),
    app_id VARCHAR(255),
    cluster_id VARCHAR(255),
    body BYTEA,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (queue_name) REFERENCES queues(name) ON DELETE CASCADE
);

CREATE INDEX idx_messages_queue_name ON messages(queue_name);
CREATE INDEX idx_messages_created_at ON messages(created_at);
```

## Troubleshooting

### Docker Not Found

```
Error: Could not find a valid Docker environment
```

**Solution**: Install Docker Desktop and ensure it's running before executing tests with `-Dpostgresql.tests.enabled=true`.

### Connection Refused

```
Connection refused: localhost:5432
```

**Solution**:
1. Check PostgreSQL is running: `pg_isready -h localhost -p 5432`
2. Verify connection details in `application.properties`
3. Check firewall settings

### Permission Denied

```
permission denied for schema public
```

**Solution**: Grant proper permissions:
```sql
GRANT ALL ON SCHEMA public TO amcp_user;
```

## Performance Tuning

For production deployments, consider these PostgreSQL optimizations:

1. **Connection Pool Settings** (in `DatabaseManager.java`):
   - `maximumPoolSize=20` - Adjust based on expected load
   - `minimumIdle=5` - Keep connections warm
   - `connectionTimeout=20000` - 20 seconds

2. **PostgreSQL Configuration**:
   ```
   max_connections = 100
   shared_buffers = 256MB
   effective_cache_size = 1GB
   work_mem = 4MB
   ```

3. **Indexes**: Already created on:
   - `messages(queue_name)` - Fast message lookup
   - `messages(created_at)` - Time-based queries
   - `bindings(exchange_name)` - Exchange routing
   - `bindings(queue_name)` - Queue bindings

## Summary

- **Development**: Install PostgreSQL or use Docker
- **Unit Tests**: Run with `mvn test` (no Docker required)
- **Integration Tests**: Run with `mvn test -Dpostgresql.tests.enabled=true` (Docker required)
- **Production**: Configure `application.properties` with your PostgreSQL connection details

The database schema is automatically managed by the application - no manual schema setup required!
