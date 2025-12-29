# Dockerfile for running AMQP 0-9-1 compliance tests with amqplib
FROM node:20-slim

WORKDIR /tests

# Copy package files and install dependencies
COPY package.json ./
RUN npm install

# Copy test files
COPY run_tests.js ./

# Environment variables
ENV AMQP_HOST=host.docker.internal
ENV AMQP_PORT=5672
ENV AMQP_USER=guest
ENV AMQP_PASS=guest

CMD ["node", "run_tests.js"]
