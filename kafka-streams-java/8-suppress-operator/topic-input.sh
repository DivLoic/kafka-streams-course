#!/usr/bin/env bash

kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic suppress-victories --property print.key=true | grep --color -E '(None|$)'