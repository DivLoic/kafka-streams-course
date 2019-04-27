#!/usr/bin/env bash

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic windowed-victories-scala

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic hopping-window-victories-scala --config cleanup.policy=compact

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic tumbling-window-victories-scala --config cleanup.policy=compact

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic session-window-victories-scala --config cleanup.policy=compact

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic sliding-window-victories-scala --config cleanup.policy=compact
