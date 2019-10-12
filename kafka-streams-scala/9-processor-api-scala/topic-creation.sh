#!/usr/bin/env bash

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic victories-scala

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic rewards-scala --config cleanup.policy=compact

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic rewarded-victories-scala