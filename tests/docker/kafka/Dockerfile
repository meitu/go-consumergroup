#use the latest, but we would override the kafka version
FROM wurstmeister/kafka:0.10.2.1
LABEL maintainer r3
ARG kafka_version=0.8.2.2
ARG scala_version=2.10
ENV KAFKA_VERSION=$kafka_version SCALA_VERSION=$scala_version
RUN rm -rf /opt/kafka*
RUN chmod a+x /tmp/download-kafka.sh && sync && /tmp/download-kafka.sh && tar xfz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt && rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka
