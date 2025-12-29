# Multi-Language AMQP Compliance Tests

This directory contains comprehensive AMQP compliance tests using official client libraries for Python, Node.js, and Go.

## Test Suites

### AMQP 0-9-1 Tests (RabbitMQ Protocol)

| Language | Library | Directory | Tests |
|----------|---------|-----------|-------|
| Python | [pika](https://github.com/pika/pika) | `python/` | 21 tests |
| Node.js | [amqplib](https://github.com/amqp-node/amqplib) | `nodejs/` | 17 tests |
| Go | [amqp091-go](https://github.com/rabbitmq/amqp091-go) | `go/` | 17 tests |

### AMQP 1.0 Tests (ISO Standard Protocol)

| Language | Library | Directory | Tests |
|----------|---------|-----------|-------|
| Python | [python-qpid-proton](https://github.com/apache/qpid-proton) | `amqp10-python/` | 15 tests |
| Node.js | [rhea](https://github.com/amqp/rhea) | `amqp10-nodejs/` | 17 tests |
| Go | [go-amqp](https://github.com/Azure/go-amqp) | `amqp10-go/` | 14 tests |

## Running Tests

### Prerequisites

- Docker and Docker Compose
- AMQP broker running on port 5672 (default)

### Run All Tests

```bash
# Linux/macOS
./run_all_tests.sh

# Windows
run_all_tests.bat
```

### Run Specific Test Suites

```bash
# Run only AMQP 0-9-1 tests
./run_all_tests.sh amqp091

# Run only AMQP 1.0 tests
./run_all_tests.sh amqp10

# Run only Python tests (both protocols)
./run_all_tests.sh python

# Run only Node.js tests (both protocols)
./run_all_tests.sh nodejs

# Run only Go tests (both protocols)
./run_all_tests.sh go
```

### Using Docker Compose Directly

```bash
# Build and run all tests
docker-compose up --build

# Run specific service
docker-compose up python-amqp091
docker-compose up nodejs-amqp10
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AMQP_HOST` | `host.docker.internal` | AMQP broker hostname |
| `AMQP_PORT` | `5672` | AMQP broker port |
| `AMQP_USER` | `guest` | AMQP username |
| `AMQP_PASS` | `guest` | AMQP password |

## Test Coverage

### AMQP 0-9-1 Tests

- Connection handling (open/close)
- Channel operations
- Queue operations (declare, bind, unbind, delete, purge)
- Exchange operations (declare, delete)
- Message publishing (basic, persistent, with headers)
- Message consuming (basic.consume, basic.get)
- Acknowledgments (ack, nack, reject)
- QoS/prefetch settings
- Transactions (tx.select, tx.commit, tx.rollback)
- Heartbeats
- Error handling

### AMQP 1.0 Tests

- Connection establishment (SASL PLAIN, ANONYMOUS)
- Session management
- Link creation (sender/receiver)
- Message transfer
- Message properties
- Application properties
- Binary payloads
- Message settlement (accept/reject/release)
- Flow control (credits)
- Large messages
- Message durability
- Message TTL
- Message priority

## Directory Structure

```
multilang/
├── docker-compose.yml          # Master compose file
├── run_all_tests.sh           # Linux/macOS runner
├── run_all_tests.bat          # Windows runner
├── README.md                  # This file
│
├── python/                    # Python AMQP 0-9-1 (pika)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── run_tests.py
│
├── nodejs/                    # Node.js AMQP 0-9-1 (amqplib)
│   ├── Dockerfile
│   ├── package.json
│   └── run_tests.js
│
├── go/                        # Go AMQP 0-9-1 (amqp091-go)
│   ├── Dockerfile
│   ├── go.mod
│   └── run_tests.go
│
├── amqp10-python/             # Python AMQP 1.0 (qpid-proton)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── run_tests.py
│
├── amqp10-nodejs/             # Node.js AMQP 1.0 (rhea)
│   ├── Dockerfile
│   ├── package.json
│   └── run_tests.js
│
└── amqp10-go/                 # Go AMQP 1.0 (go-amqp)
    ├── Dockerfile
    ├── go.mod
    └── run_tests.go
```

## Adding New Tests

Each test file follows a common pattern:
1. Connect to the broker
2. Perform the test operation
3. Verify the result
4. Clean up resources
5. Report success/failure

See existing test files for examples of how to add new tests.
