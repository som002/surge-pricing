#!/usr/bin/env python3
"""
start_cluster.py — Python equivalent of start-cluster.sh
Starts a 3-broker Kafka KRaft cluster on a single machine.
"""

import os
import sys
import signal
import subprocess
import time

# ── Config ─────────────────────────────────────────────────────────────────
KAFKA_DIR = os.path.dirname(os.path.abspath(__file__))

CONFIGS = [
    "config/server-1.properties",
    "config/server-2.properties",
    "config/server-3.properties",
]

LOG_DIRS = [
    "/tmp/kraft-logs-1",
    "/tmp/kraft-logs-2",
    "/tmp/kraft-logs-3",
]

CLUSTER_ID_FILE = "/tmp/kafka-cluster-id"

pids: list[subprocess.Popen] = []


# ── Cleanup ─────────────────────────────────────────────────────────────────
def cleanup():
    print("\n==> Shutting down all brokers...")
    for proc in pids:
        try:
            proc.terminate()
            print(f"    Stopped PID {proc.pid}")
        except ProcessLookupError:
            pass  # already dead
    for proc in pids:
        proc.wait()
    print("==> All brokers stopped.")


def handle_signal(signum, frame):
    cleanup()
    sys.exit(0)


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


# ── Step 1: Generate or reuse cluster ID ────────────────────────────────────
if os.path.isfile(CLUSTER_ID_FILE):
    with open(CLUSTER_ID_FILE, "r") as f:
        cluster_id = f.read().strip()
    print(f"==> Reusing cluster ID: {cluster_id}")
else:
    result = subprocess.run(
        [f"{KAFKA_DIR}/bin/kafka-storage.sh", "random-uuid"],
        capture_output=True,
        text=True,
        check=True,
    )
    cluster_id = result.stdout.strip()
    with open(CLUSTER_ID_FILE, "w") as f:
        f.write(cluster_id)
    print(f"==> Generated new cluster ID: {cluster_id}")


# ── Step 2: Format storage for each broker (only if not already done) ────────
for i, (log_dir, config) in enumerate(zip(LOG_DIRS, CONFIGS)):
    broker_num = i + 1
    meta_props = os.path.join(log_dir, "meta.properties")

    if os.path.isfile(meta_props):
        print(f"==> Broker {broker_num}: storage already formatted, skipping.")
    else:
        print(f"==> Broker {broker_num}: formatting storage at {log_dir}...")
        subprocess.run(
            [
                f"{KAFKA_DIR}/bin/kafka-storage.sh", "format",
                "-t", cluster_id,
                "-c", f"{KAFKA_DIR}/{config}",
            ],
            check=True,
        )
        print(f"==> Broker {broker_num}: formatted.")


# ── Step 3: Start all brokers in background ──────────────────────────────────
for i, config in enumerate(CONFIGS):
    broker_num = i + 1
    log_file = f"/tmp/kafka-broker-{broker_num}.log"

    print(f"==> Starting broker {broker_num} (log: {log_file})...")
    with open(log_file, "w") as lf:
        proc = subprocess.Popen(
            [f"{KAFKA_DIR}/bin/kafka-server-start.sh", f"{KAFKA_DIR}/{config}"],
            stdout=lf,
            stderr=subprocess.STDOUT,
        )
    pids.append(proc)
    print(f"    PID: {proc.pid}")
    time.sleep(1)  # stagger startup slightly


print()
print("==> 3-broker cluster is up!")
print("    Bootstrap servers: localhost:9092,localhost:9192,localhost:9292")
print("    Logs: /tmp/kafka-broker-{1,2,3}.log")
print("    Press Ctrl+C to stop all brokers.")
print()


# ── Wait for any broker to exit, then trigger cleanup ───────────────────────
try:
    while True:
        for proc in pids:
            if proc.poll() is not None:  # process has exited
                print("==> A broker exited. Triggering cleanup...")
                cleanup()
                sys.exit(0)
        time.sleep(1)
except Exception:
    cleanup()
    sys.exit(1)
