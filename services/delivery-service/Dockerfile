# A small image with just Java 21 (what Ballerina programs run on)
FROM eclipse-temurin:21-jre

# Work inside /app in the container
WORKDIR /app

# Copy the built program into the container
COPY target/bin/delivery_service.jar app.jar

# Document which port the service listens on
EXPOSE 8085

# Start the service
ENTRYPOINT ["java", "-jar", "app.jar"]