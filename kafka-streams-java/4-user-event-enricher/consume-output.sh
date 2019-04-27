#!/usr/bin/env bash

#kafka-console-consumer --bootstrap-server localhost:9092 \
#    --topic user-purchases-enriched-left-join \
#    --from-beginning \
#    --property print.key=true \
#    --property print.value=true

kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic user-purchases-enriched-inner-join \
    --from-beginning \
    --property print.key=true \
    --property print.value=true