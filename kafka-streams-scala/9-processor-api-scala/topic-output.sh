#!/usr/bin/env bash

kafka-avro-console-consumer --bootstrap-server localhost:9092 \
    --topic rewarded-victories-scala \
    --property print.key=true \
    --property print.value=true
