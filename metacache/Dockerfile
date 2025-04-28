# Use a JDK base image
FROM openjdk:17-jdk-alpine

# Set the working directory
WORKDIR /app

# Copy the JAR from the build context
COPY build/libs/*.jar app.jar

# Expose port
EXPOSE 8080

# Run the JAR
CMD ["java", "-jar", "app.jar"]