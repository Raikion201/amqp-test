# Multi-stage build for AMQP Server
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build

# Copy pom.xml first for dependency caching
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source and build
COPY src ./src
RUN mvn package -DskipTests -B

# Runtime image
FROM eclipse-temurin:17-jre-alpine

LABEL maintainer="AMQP Server"
LABEL description="AMQP 0-9-1 and 1.0 Message Broker"

# Create non-root user
RUN addgroup -S amqp && adduser -S amqp -G amqp

WORKDIR /opt/amqp

# Copy the built jar
COPY --from=builder /build/target/*.jar app.jar

# Create directories for data and config
RUN mkdir -p /var/lib/amqp /etc/amqp /var/log/amqp \
    && chown -R amqp:amqp /var/lib/amqp /etc/amqp /var/log/amqp /opt/amqp

# Default configuration
ENV AMQP_DATA_DIR=/var/lib/amqp \
    AMQP_LOG_DIR=/var/log/amqp \
    AMQP_PORT=5672 \
    AMQP_PORT_10=5671 \
    AMQP_MANAGEMENT_PORT=15672 \
    JAVA_OPTS="-Xmx512m -Xms256m"

# Expose ports
# 5672: AMQP 0-9-1
# 5671: AMQP 1.0
# 5673: AMQP 0-9-1 TLS
# 5674: AMQP 1.0 TLS
# 15672: Management/HTTP
EXPOSE 5672 5671 5673 5674 15672

# Switch to non-root user
USER amqp

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD wget -q --spider http://localhost:15672/health || exit 1

ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar app.jar"]
