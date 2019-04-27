#!/usr/bin/env bash

echo "Try to type:"
echo "player1 vs player2"
echo "ChunLi vs Ryu"
echo "Blair vs Allen"

kafka-console-producer --broker-list localhost:9092 --topic word-count-input