#!/usr/bin/env bash

echo "try to type:"
echo "Ken vs Ryu"
echo "Blair vs Ken"
echo "Allen vs Ryu"

kafka-console-producer --broker-list localhost:9092 --topic streams-plaintext-input
