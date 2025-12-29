#!/bin/bash
# Master script to run all AMQP compliance tests
#
# Usage:
#   ./run_all_tests.sh                  # Run all tests
#   ./run_all_tests.sh amqp091          # Run only AMQP 0-9-1 tests
#   ./run_all_tests.sh amqp10           # Run only AMQP 1.0 tests
#   ./run_all_tests.sh python           # Run only Python tests
#   ./run_all_tests.sh nodejs           # Run only Node.js tests
#   ./run_all_tests.sh go               # Run only Go tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Default environment variables
export AMQP_HOST="${AMQP_HOST:-host.docker.internal}"
export AMQP_PORT="${AMQP_PORT:-5672}"
export AMQP_USER="${AMQP_USER:-guest}"
export AMQP_PASS="${AMQP_PASS:-guest}"

echo "=============================================="
echo "AMQP Compliance Test Suite"
echo "=============================================="
echo "Broker: $AMQP_HOST:$AMQP_PORT"
echo "User: $AMQP_USER"
echo "=============================================="

FILTER="$1"
FAILED=0

run_test() {
    local service=$1
    local name=$2
    echo ""
    echo "----------------------------------------------"
    echo "Running: $name"
    echo "----------------------------------------------"

    if docker-compose build "$service" 2>/dev/null && docker-compose up --exit-code-from "$service" "$service" 2>/dev/null; then
        echo "✓ $name: PASSED"
    else
        echo "✗ $name: FAILED"
        FAILED=$((FAILED + 1))
    fi
}

# AMQP 0-9-1 Tests
if [[ -z "$FILTER" || "$FILTER" == "amqp091" || "$FILTER" == "python" ]]; then
    run_test "python-amqp091" "Python AMQP 0-9-1 (pika)"
fi

if [[ -z "$FILTER" || "$FILTER" == "amqp091" || "$FILTER" == "nodejs" ]]; then
    run_test "nodejs-amqp091" "Node.js AMQP 0-9-1 (amqplib)"
fi

if [[ -z "$FILTER" || "$FILTER" == "amqp091" || "$FILTER" == "go" ]]; then
    run_test "go-amqp091" "Go AMQP 0-9-1 (amqp091-go)"
fi

# AMQP 1.0 Tests
if [[ -z "$FILTER" || "$FILTER" == "amqp10" || "$FILTER" == "nodejs" ]]; then
    run_test "nodejs-amqp10" "Node.js AMQP 1.0 (rhea)"
fi

if [[ -z "$FILTER" || "$FILTER" == "amqp10" || "$FILTER" == "python" ]]; then
    run_test "python-amqp10" "Python AMQP 1.0 (qpid-proton)"
fi

if [[ -z "$FILTER" || "$FILTER" == "amqp10" || "$FILTER" == "go" ]]; then
    run_test "go-amqp10" "Go AMQP 1.0 (go-amqp)"
fi

echo ""
echo "=============================================="
echo "TEST SUITE COMPLETE"
echo "=============================================="

if [ $FAILED -gt 0 ]; then
    echo "✗ $FAILED test suite(s) failed"
    exit 1
else
    echo "✓ All test suites passed"
    exit 0
fi
