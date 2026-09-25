# ==============================================================================
# Multi-stage Dockerfile for TaskFlow (Distributed Task Engine)
# ==============================================================================

# Stage 1: Build artifact using Maven and Temurin JDK 17
FROM maven:3.9.6-eclipse-temurin-17-alpine AS builder

WORKDIR /build

# Copy dependency definition for layer caching
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy application source tree
COPY src ./src

# Compile and package application jar
RUN mvn clean package -DskipTests -B

# Stage 2: Minimal, security-hardened Alpine runtime
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Create a non-root system group and user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Copy built artifact from builder stage
COPY --from=builder /build/target/*.jar app.jar

# Enforce secure ownership
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

EXPOSE 8080

# Run with container-aware memory allocation flags
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
