#!/usr/bin/env bash
set -euo pipefail

PID_FILE="/tmp/kafka-cluster.pids"

echo "==> Kafka stop invoked at $(date)"

# Stop watchdog first (important!)
if pgrep -f kafka-watchdog.sh >/dev/null; then
  echo "Stopping Kafka watchdog..."
  pkill -f kafka-watchdog.sh
fi

# Trigger graceful shutdown
if [ -f "$PID_FILE" ]; then
  kill -TERM $(cat "$PID_FILE")
  echo "SIGTERM sent to Kafka brokers"
else
  echo "PID file not found; Kafka already stopped"
fi