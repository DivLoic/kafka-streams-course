#!/usr/bin/env bash

# create input topic with two partitions
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic word-count-input

# create output topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic word-count-output-scala