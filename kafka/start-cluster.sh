#!/usr/bin/env bash
# start-cluster.sh — Start a 3-broker Kafka KRaft cluster
# All brokers share the same CLUSTER_ID and are formatted on first run.
# Supports graceful (SIGTERM → wait → SIGKILL) shutdown with PID persistence.

set -euo pipefail

KAFKA_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIGS=("config/server-1.properties" "config/server-2.properties" "config/server-3.properties")
LOG_DIRS=("/tmp/kraft-logs-1" "/tmp/kraft-logs-2" "/tmp/kraft-logs-3")
CLUSTER_ID_FILE="/tmp/kafka-cluster-id"
PID_FILE="/tmp/kafka-cluster.pids"

# How long (seconds) to wait for graceful SIGTERM before escalating to SIGKILL
STOP_TIMEOUT=30

PIDS=()
_SHUTDOWN_DONE=0   # guard: prevent double-cleanup from EXIT + SIGINT/SIGTERM

# ── Graceful stop ─────────────────────────────────────────────────────────────
graceful_stop() {
  local pid=$1
  local broker_label=$2

  # Try the Kafka-native stop script first (flushes logs, runs shutdown hooks)
  if "$KAFKA_DIR/bin/kafka-server-stop.sh" 2>/dev/null; then
    :  # stop script sent the signal; fall through to wait logic below
  else
    # Fallback: send SIGTERM directly
    kill -TERM "$pid" 2>/dev/null || true
  fi

  # Wait up to STOP_TIMEOUT seconds for graceful exit
  local elapsed=0
  while kill -0 "$pid" 2>/dev/null; do
    if (( elapsed >= STOP_TIMEOUT )); then
      echo "    [WARN] $broker_label (PID $pid) did not stop after ${STOP_TIMEOUT}s — escalating to SIGKILL"
      kill -KILL "$pid" 2>/dev/null || true
      break
    fi
    sleep 1
    (( elapsed++ )) || true
  done

  # Reap the process to avoid zombies
  wait "$pid" 2>/dev/null || true
  echo "    Stopped $broker_label (PID $pid)"
}

# ── Cleanup handler ───────────────────────────────────────────────────────────
cleanup() {
  # Guard against double invocation (EXIT fires after SIGINT/SIGTERM handlers)
  [[ $_SHUTDOWN_DONE -eq 1 ]] && return
  _SHUTDOWN_DONE=1

  echo ""
  echo "==> Shutting down all brokers gracefully (timeout: ${STOP_TIMEOUT}s each)..."

  # Reload PIDs from file in case this is a re-entrant call
  if [[ ${#PIDS[@]} -eq 0 && -f "$PID_FILE" ]]; then
    mapfile -t PIDS < "$PID_FILE"
  fi

  local i=0
  for pid in "${PIDS[@]}"; do
    local broker_num=$(( i + 1 ))
    if kill -0 "$pid" 2>/dev/null; then
      echo "    Stopping broker $broker_num (PID $pid)..."
      graceful_stop "$pid" "Broker $broker_num"
    else
      echo "    Broker $broker_num (PID $pid) already stopped."
    fi
    (( i++ )) || true
  done

  # Remove PID file after clean shutdown
  rm -f "$PID_FILE"
  echo "==> All brokers stopped."
}

trap 'cleanup; exit 0' SIGINT SIGTERM
trap 'cleanup'          EXIT

# ── Step 1: Generate or reuse a shared cluster ID ────────────────────────────
if [ -f "$CLUSTER_ID_FILE" ]; then
  CLUSTER_ID=$(cat "$CLUSTER_ID_FILE")
  echo "==> Reusing cluster ID: $CLUSTER_ID"
else
  CLUSTER_ID=$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)
  echo "$CLUSTER_ID" > "$CLUSTER_ID_FILE"
  echo "==> Generated new cluster ID: $CLUSTER_ID"
fi

# ── Step 2: Format storage for each broker (only if not already done) ─────────
for i in 0 1 2; do
  BROKER_NUM=$((i + 1))
  LOG_DIR="${LOG_DIRS[$i]}"
  CONFIG="${CONFIGS[$i]}"
  META_PROPS="$LOG_DIR/meta.properties"

  if [ -f "$META_PROPS" ]; then
    echo "==> Broker $BROKER_NUM: storage already formatted, skipping."
  else
    echo "==> Broker $BROKER_NUM: formatting storage at $LOG_DIR..."
    "$KAFKA_DIR/bin/kafka-storage.sh" format \
      -t "$CLUSTER_ID" \
      -c "$KAFKA_DIR/$CONFIG"
    echo "==> Broker $BROKER_NUM: formatted."
  fi
done

# ── Step 3: Start all brokers in background ────────────────────────────────────
# Truncate PID file before writing fresh PIDs
> "$PID_FILE"

for i in 0 1 2; do
  BROKER_NUM=$((i + 1))
  CONFIG="${CONFIGS[$i]}"
  LOG_FILE="/tmp/kafka-broker-${BROKER_NUM}.log"

  echo "==> Starting broker $BROKER_NUM (log: $LOG_FILE)..."
  "$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/$CONFIG" >> "$LOG_FILE" 2>&1 &
  pid=$!
  PIDS+=("$pid")
  echo "$pid" >> "$PID_FILE"
  echo "    PID: $pid"
  sleep 1   # stagger startup slightly
done

echo ""
echo "==> 3-broker cluster is up!"
echo "    Bootstrap servers: localhost:9092,localhost:9192,localhost:9292"
echo "    Log files:         /tmp/kafka-broker-{1,2,3}.log"
echo "    PID file:          $PID_FILE"
echo "    Press Ctrl+C to gracefully stop all brokers."
echo ""

# ── Step 4: Monitor — wait for ALL brokers; detect early exits ────────────────
all_alive=true
while $all_alive; do
  all_alive=false
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      all_alive=true
    fi
  done
  $all_alive && sleep 2
done

echo "==> One or more brokers exited unexpectedly. Triggering shutdown..."
# cleanup will be called automatically by the EXIT trap
