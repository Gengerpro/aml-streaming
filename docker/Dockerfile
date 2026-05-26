FROM maven:3.9-eclipse-temurin-8 AS build
WORKDIR /app
COPY pom.xml ./
COPY aml-common/pom.xml aml-common/
COPY aml-streaming/pom.xml aml-streaming/
COPY aml-batch/pom.xml aml-batch/
COPY aml-service/pom.xml aml-service/
RUN mvn dependency:go-offline -B 2>/dev/null || true
COPY . .
RUN mvn package -pl aml-service -am -DskipTests -B

FROM eclipse-temurin:8-jre
WORKDIR /app
COPY --from=build /app/aml-service/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
