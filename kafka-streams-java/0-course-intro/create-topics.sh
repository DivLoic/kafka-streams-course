#!/usr/bin/env bash

# create input topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-plaintext-input

# create output topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-wordcount-output