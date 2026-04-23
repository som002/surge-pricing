from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

PID_FILE = "/home/somyan/surge.pid"

STOP_COMMAND = f"""
if [ -f {PID_FILE} ]; then
  PID=$(cat {PID_FILE})
  if kill -0 $PID 2>/dev/null; then
    echo "🛑 Sending SIGTERM to surge pricing (PID=$PID)"
    kill -TERM $PID

    for i in {{1..30}}; do
      if ! kill -0 $PID 2>/dev/null; then
        echo "✅ Surge pricing stopped gracefully"
        rm -f {PID_FILE}
        exit 0
      fi
      sleep 1
    done

    echo "⚠️ WARNING: Process did not stop within 30 seconds"
  else
    echo "ℹ️ PID file exists but process is not running"
    rm -f {PID_FILE}
  fi
else
  echo "ℹ️ No running surge pricing process found"
fi
"""

with DAG(
    dag_id="surge_pricing_stop",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kafka", "surge_pricing", "stop"],
) as dag:

    stop_surge = SSHOperator(
        task_id="stop_surge_pricing",
        ssh_conn_id="ssh_default",
        command=STOP_COMMAND,
    )