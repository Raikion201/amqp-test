FROM node:18-alpine

WORKDIR /tests

# Copy package files
COPY package.json ./

# Install dependencies
RUN npm install

# Copy test files
COPY run_tests.js ./

# Set default environment variables
ENV AMQP_HOST=host.docker.internal
ENV AMQP_PORT=5672
ENV AMQP_USER=guest
ENV AMQP_PASS=guest

# Run tests
CMD ["npm", "test"]
