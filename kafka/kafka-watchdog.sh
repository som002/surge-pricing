#!/usr/bin/env bash
set -euo pipefail

KAFKA_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="/home/somyan/kafka-watchdog.log"
PID_FILE="/tmp/kafka-cluster.pids"
INTERVAL=30

echo "[INFO] Kafka watchdog started at $(date)" >> "$LOG"

while true; do
  if [ ! -f "$PID_FILE" ] || ! pgrep -F "$PID_FILE" >/dev/null 2>&1; then
    echo "[WARN] Kafka appears down at $(date). Restarting..." >> "$LOG"

    nohup bash "$KAFKA_DIR/start-cluster.sh" \
      >> /home/somyan/kafka.log 2>&1 &

    sleep 20
  fi

  sleep "$INTERVAL"
done