# Multi-stage build for AMQP Server
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /app
COPY pom.xml .
COPY src ./src

# Build the shaded JAR
RUN mvn clean package -DskipTests -q

# Runtime image
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Copy the shaded JAR
COPY --from=builder /app/target/amqp-server-1.0.0.jar ./amqp-server.jar

# Create data directory for persistence and install netcat for healthcheck
RUN mkdir -p /app/data && apk add --no-cache netcat-openbsd

# Expose AMQP 0-9-1, AMQP 1.0, and management ports
EXPOSE 5672 5671 15672

# Health check on AMQP 0-9-1 port
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD nc -z localhost 5672 || exit 1

# Environment variables for database configuration and guest user
ENV AMQP_DB_URL="jdbc:h2:mem:amqp;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
ENV AMQP_DB_USER="sa"
ENV AMQP_DB_PASSWORD=""
ENV AMQP_GUEST_USER="true"

# Run the server with H2 in-memory database and both AMQP protocols enabled
# AMQP 0-9-1 on port 5672, AMQP 1.0 on port 5671 (without SASL for testing)
ENTRYPOINT ["java", "-Xmx512m", "-Xms256m", "-jar", "amqp-server.jar", \
            "--db-url", "jdbc:h2:mem:amqp;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false", \
            "--db-user", "sa", \
            "--db-password", "", \
            "--enable-amqp10", \
            "--amqp10-port", "5671", \
            "--amqp10-no-sasl"]
