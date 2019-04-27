#!/usr/bin/env bash

# create input topic with one partition to get full ordering
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic arcade-contest

# create output log compacted topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic arcade-contest-exactly-once-scala --config cleanup.policy=compact