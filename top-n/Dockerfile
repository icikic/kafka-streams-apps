FROM openjdk:8-jre-slim

ENV JAVA_OPTS="-server -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -XX:MaxRAMFraction=1" \
    DOCKERIZE_VERSION=v0.6.0

RUN set -ex && \
    apt-get update && \
    apt-get install -y curl && \
    apt-get clean && \
    curl --fail --retry 3 --retry-delay 1 -L \
        https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
        | tar xzv -C /usr/local/bin && \
    mkdir -p /resources /jars /data


COPY target/top-n-*standalone.jar /jars/top-n-standalone.jar
