#!/usr/bin/env bash

kafka-avro-console-consumer --bootstrap-server localhost:9092 \
    --topic session-window-victories \
    --property print.key=true \
    --property print.value=true | grep --color -E '(count|$)'
