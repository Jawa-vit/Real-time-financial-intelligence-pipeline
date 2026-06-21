#!/bin/bash
# kafka/create_topics.sh
# Creates the topics this pipeline needs. Run after `docker compose up -d kafka`.
#
# Usage: ./kafka/create_topics.sh
set -e

BROKER="${KAFKA_BROKER:-localhost:9092}"
CONTAINER="${KAFKA_CONTAINER:-finance-kafka}"

create_topic() {
  local topic=$1
  local partitions=$2
  echo "Creating topic: $topic (partitions=$partitions)"
  docker exec "$CONTAINER" kafka-topics --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1
}

create_topic "finance-raw-stream" 3
create_topic "finance-processed-stream" 3

echo "Listing topics:"
docker exec "$CONTAINER" kafka-topics --list --bootstrap-server localhost:9092
