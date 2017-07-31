#!/bin/sh
docker-compose -p go_consumergroup_test up --build -d

docker-compose -p go_consumergroup_test exec -T zookeeper sh -c 'until nc -z 127.0.0.1 2181; do echo "zk is not ready"; sleep 1; done'
docker-compose -p go_consumergroup_test exec -T kafka sh -c 'until nc -z 127.0.0.1 9092; do echo "kafka is not ready"; sleep 1; done'
docker-compose -p go_consumergroup_test exec -T kafka sh -c 'until kafka-topics.sh --list --zookeeper zookeeper|grep test|grep -v grep; do echo "kafka topic is not ready"; sleep 1; done'
