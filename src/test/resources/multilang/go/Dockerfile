# Dockerfile for running AMQP 0-9-1 compliance tests with amqp091-go
FROM golang:1.21-alpine

WORKDIR /tests

# Copy go module files
COPY go.mod ./

# Download dependencies
RUN go mod download || go mod tidy && go mod download

# Copy test files
COPY run_tests.go ./

# Build the test binary
RUN go build -o run_tests .

# Set default environment variables
ENV AMQP_HOST=host.docker.internal
ENV AMQP_PORT=5672
ENV AMQP_USER=guest
ENV AMQP_PASS=guest

# Run tests
CMD ["./run_tests"]
