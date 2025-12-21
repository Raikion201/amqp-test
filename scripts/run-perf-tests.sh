#!/bin/bash
# Run performance tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Test parameters
PRODUCERS=${PRODUCERS:-4}
CONSUMERS=${CONSUMERS:-4}
RATE=${RATE:-10000}
DURATION=${DURATION:-60}
MESSAGE_SIZE=${MESSAGE_SIZE:-256}

cd "$PROJECT_DIR/docker"

echo "=== Starting AMQP server ==="
docker-compose up -d amqp-server

# Wait for server
echo "Waiting for server..."
sleep 10

echo "=== Running performance tests ==="
echo "Producers: $PRODUCERS"
echo "Consumers: $CONSUMERS"
echo "Target rate: $RATE msg/s"
echo "Duration: $DURATION seconds"
echo "Message size: $MESSAGE_SIZE bytes"
echo ""

# Run PerfTest
docker-compose run --rm perf-test \
    --uri amqp://guest:guest@amqp-server:5672 \
    --producers $PRODUCERS \
    --consumers $CONSUMERS \
    --rate $RATE \
    --time $DURATION \
    --size $MESSAGE_SIZE \
    --confirm 100 \
    --multi-ack-every 100 \
    --queue-pattern "perf-test-%d" \
    --queue-pattern-from 1 \
    --queue-pattern-to 4 \
    --json-body

echo "=== Performance test complete ==="

# Show server stats
echo ""
echo "=== Server Statistics ==="
docker-compose exec -T amqp-server wget -q -O - http://localhost:15672/api/overview 2>/dev/null | head -20 || echo "(Stats not available)"

echo ""
echo "=== Cleaning up ==="
docker-compose down
