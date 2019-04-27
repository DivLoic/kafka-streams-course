#!/usr/bin/env bash

# create input topic for user purchases
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user-purchases

# create table of user information - log compacted for optimisation
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic user-table --config cleanup.policy=compact

# create out topic for user purchases enriched with user data (left join)
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user-purchases-enriched-left-join

# create out topic for user purchases enriched with user data (inner join)
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic user-purchases-enriched-inner-join