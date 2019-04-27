#!/usr/bin/env bash

kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic windowed-victories-scala --property print.key=true