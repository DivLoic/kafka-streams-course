#!/usr/bin/env bash

kafka-avro-console-consumer --bootstrap-server localhost:9092 \
    --topic suppress-errors-count \
    --property print.key=true \
    --property print.value=true
