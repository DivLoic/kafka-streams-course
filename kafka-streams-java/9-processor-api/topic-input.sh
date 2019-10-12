#!/usr/bin/env bash

kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic victories --property print.key=true