#!/usr/bin/env bash
set -euo pipefail

KAFKA_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Airflow Kafka start invoked at $(date)"

# Start Kafka cluster if not running
if ! pgrep -f kafka.Kafka >/dev/null; then
  nohup bash "$KAFKA_DIR/start-cluster.sh" \
    >> /home/somyan/kafka.log 2>&1 &
  echo "Kafka start-cluster launched"
else
  echo "Kafka already running"
fi

# Start watchdog if not running
if ! pgrep -f kafka-watchdog.sh >/dev/null; then
  nohup bash "$KAFKA_DIR/kafka-watchdog.sh" \
    >> /home/somyan/kafka-watchdog.log 2>&1 &
  echo "Kafka watchdog started"
else
  echo "Kafka watchdog already running"
fi