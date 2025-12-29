FROM python:3.11-slim

WORKDIR /tests

# Install build dependencies for qpid-proton
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    libssl-dev \
    libsasl2-dev \
    swig \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy test files
COPY run_tests.py ./

# Set default environment variables
ENV AMQP_HOST=host.docker.internal
ENV AMQP_PORT=5672
ENV AMQP_USER=guest
ENV AMQP_PASS=guest

# Run tests
CMD ["python", "run_tests.py"]
