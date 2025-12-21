#!/bin/bash
# Run all tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "=== Building project ==="
mvn clean package -DskipTests -q

echo "=== Running unit tests ==="
mvn test -q

echo "=== Running integration tests ==="
mvn verify -DskipUTs -q || true

echo "=== Starting Docker environment ==="
cd docker
docker-compose up -d amqp-server

# Wait for server to be healthy
echo "Waiting for AMQP server..."
timeout=60
while [ $timeout -gt 0 ]; do
    if docker-compose exec -T amqp-server wget -q --spider http://localhost:15672/health 2>/dev/null; then
        echo "Server is healthy!"
        break
    fi
    sleep 2
    timeout=$((timeout - 2))
done

if [ $timeout -le 0 ]; then
    echo "Server failed to start"
    docker-compose logs amqp-server
    exit 1
fi

echo "=== Running external tests ==="
docker-compose -f docker-compose.yml -f docker-compose.test.yml up --abort-on-container-exit qpid-test-runner java-test-runner

echo "=== Collecting test results ==="
mkdir -p "$PROJECT_DIR/test-results"
cp -r test-results/* "$PROJECT_DIR/test-results/" 2>/dev/null || true

echo "=== Cleaning up ==="
docker-compose down

echo "=== Tests complete ==="
echo "Results available in: $PROJECT_DIR/test-results/"
