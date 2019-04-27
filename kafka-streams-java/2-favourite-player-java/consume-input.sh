#!/usr/bin/env bash

# start a consumer on the output topic
kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic favourite-player-input \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer