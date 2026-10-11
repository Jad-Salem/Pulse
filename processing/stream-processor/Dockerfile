# syntax=docker/dockerfile:1.7
# Build context: repository root (the build needs schemas/avro).
FROM maven:3.9.16-eclipse-temurin-17 AS build
WORKDIR /build
COPY schemas/avro schemas/avro
COPY processing/stream-processor/pom.xml processing/stream-processor/pom.xml
COPY processing/stream-processor/src processing/stream-processor/src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -f processing/stream-processor/pom.xml package -DskipTests

# Application mode: the job jar is baked into the image, and the JobManager runs exactly this job.
FROM flink:2.2.1-java17
RUN mkdir -p /opt/flink/usrlib /opt/flink/data/checkpoints /opt/flink/data/savepoints \
    && chown -R flink:flink /opt/flink/usrlib /opt/flink/data
COPY --from=build --chown=flink:flink /build/processing/stream-processor/target/stream-processor.jar /opt/flink/usrlib/stream-processor.jar
