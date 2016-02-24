#!/bin/bash
FILES=~/clean_data/all/*
for f in $FILES
do
  echo "Sending to kafka file $f"
  ~/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test4 < $f
  sleep 2
done
