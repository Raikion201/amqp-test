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

# Create data directory for persistence
RUN mkdir -p /app/data

# Expose AMQP port and management port
EXPOSE 5672 15672

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD nc -z localhost 5672 || exit 1

# Environment variables for database configuration and guest user
ENV AMQP_DB_URL="jdbc:h2:mem:amqp;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
ENV AMQP_DB_USER="sa"
ENV AMQP_DB_PASSWORD=""
ENV AMQP_GUEST_USER="true"

# Run the server with H2 in-memory database by default
ENTRYPOINT ["java", "-Xmx512m", "-Xms256m", "-jar", "amqp-server.jar", \
            "--db-url", "jdbc:h2:mem:amqp;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false", \
            "--db-user", "sa", \
            "--db-password", ""]
