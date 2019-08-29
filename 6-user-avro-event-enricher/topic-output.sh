#!/usr/bin/env bash

#    --topic specific-avro-purchases \
kafka-avro-console-consumer --bootstrap-server localhost:9092\
    --topic generic-avro-purchases \
    --from-beginning \
    --property print.key=true \
    --property print.value=true
