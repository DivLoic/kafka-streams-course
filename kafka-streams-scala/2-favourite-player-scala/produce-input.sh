#!/usr/bin/env bash

echo "Try to type:"
echo "terminalA,Ken"
echo "terminalB,Dalshim"
echo "terminalC,Skullomania"

kafka-console-producer --broker-list localhost:9092 --topic favourite-player-input