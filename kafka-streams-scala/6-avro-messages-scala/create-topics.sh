#!/usr/bin/env bash

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic avro-user-purchases

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic avro-user-table --config cleanup.policy=compact

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic generic-avro-purchases-scala

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic specific-avro-purchases-scala