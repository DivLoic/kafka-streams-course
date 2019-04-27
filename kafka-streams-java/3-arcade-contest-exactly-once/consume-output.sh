#!/usr/bin/env bash

# launch a Kafka consumer
kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic arcade-contest-exactly-once \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer | grep --color -E '"life-points":0|$'