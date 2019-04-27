#!/usr/bin/env bash

# create input topic with one partition to get full ordering
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic favourite-player-input

# create intermediary log compacted topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic terminal-keys-and-players --config cleanup.policy=compact

# create output log compacted topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic favourite-player-output --config cleanup.policy=compact