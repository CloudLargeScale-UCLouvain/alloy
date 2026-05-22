FROM maven:3.9-eclipse-temurin-11
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src/ src/
RUN mvn package -DskipTests -q
CMD ["cp", "target/flink-examples-1.0-SNAPSHOT.jar", "/output/flink-examples.jar"]
