#!/usr/bin/env bash

kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic victories-scala --property print.key=true