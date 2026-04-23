from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

PID_FILE = "/home/somyan/surge.pid"
LOG_FILE = "/home/somyan/surge.log"
PYTHON_BIN = "/home/somyan/.venv/bin/python3"
SCRIPT = "/home/somyan/surge.py"

START_COMMAND = f"""
if [ ! -f {PID_FILE} ] || ! kill -0 $(cat {PID_FILE}) 2>/dev/null; then
  nohup {PYTHON_BIN} {SCRIPT} > {LOG_FILE} 2>&1 &
  echo $! > {PID_FILE}
  echo "✅ Surge pricing started with PID $(cat {PID_FILE})"
else
  echo "ℹ️ Surge pricing already running with PID $(cat {PID_FILE})"
fi
"""

with DAG(
    dag_id="surge_pricing_start",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kafka", "surge_pricing", "start"],
) as dag:

    start_surge = SSHOperator(
        task_id="start_surge_pricing",
        ssh_conn_id="ssh_default",
        command=START_COMMAND,
    )




# from airflow import DAG
# from airflow.providers.ssh.operators.ssh import SSHOperator
# from datetime import datetime

# with DAG(
#     dag_id="surge_pricing",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["kafka", "surge_pricing"],
# ) as dag:

#     surge_s = SSHOperator(
#         task_id="surge_s",
#         ssh_conn_id="ssh_default",
#         command=(
#             "nohup .venv/bin/python3 /home/somyan/surge.py "
#             "> /home/somyan/surge.log 2>&1 &"
#         ),
#     )