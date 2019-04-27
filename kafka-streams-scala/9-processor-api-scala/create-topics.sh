#!/usr/bin/env bash

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic processor-victories

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic processor-rewards-scala --config cleanup.policy=compact

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic processor-rewarded-victories-scala