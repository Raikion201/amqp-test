# Dockerfile for running AMQP 0-9-1 compliance tests with pika
FROM python:3.11-slim

WORKDIR /tests

# Install pika
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy our test runner script
COPY run_tests.py ./

# Environment variables for AMQP connection
ENV AMQP_HOST=host.docker.internal
ENV AMQP_PORT=5672
ENV AMQP_USER=guest
ENV AMQP_PASS=guest

# Default command runs the tests
CMD ["python", "run_tests.py"]
