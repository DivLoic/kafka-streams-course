#!/usr/bin/env bash

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic suppress-victories

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic suppress-errors-count-scala