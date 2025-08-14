# ================================
# 1️⃣ Build stage
# ================================
FROM maven:3.9.9-eclipse-temurin-22 AS build

WORKDIR /app

COPY pom.xml .
RUN mvn dependency:go-offline

COPY src ./src
RUN mvn clean package -DskipTests

# ================================
# 2️⃣ Runtime stage
# ================================
FROM openjdk:22-jdk-slim

WORKDIR /app

COPY --from=build /app/target/*.jar app.jar

# Use Render's assigned port
ENV PORT=$PORT
EXPOSE $PORT

ENTRYPOINT ["java", "-jar", "app.jar", "--server.port=${PORT}"]
