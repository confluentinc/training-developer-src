FROM --platform=$BUILDPLATFORM gradle:8.12-jdk21 AS builder
WORKDIR /home/gradle/project
COPY build.gradle ./
COPY src ./src
USER root
RUN gradle build

FROM eclipse-temurin:21-jre
RUN apt-get update && \
    apt-get install -y curl kafkacat && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /home/gradle/project/build/distributions/project.tar /app/
COPY drivers /app/project/drivers
WORKDIR /app
RUN tar -xvf project.tar
WORKDIR /app/project
CMD java -classpath "lib/*" clients.Producer
