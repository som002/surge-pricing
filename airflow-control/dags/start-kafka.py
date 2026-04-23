from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# DAG 1 — START Kafka (manual, run ONCE)
# ─────────────────────────────────────────────
with DAG(
    dag_id="kafka_cluster_start",
    start_date=datetime(2024, 1, 1),
    schedule=None,              # ✅ Manual
    catchup=False,
    tags=["kafka", "infra"],
) as kafka_start_dag:

    start_kafka = SSHOperator(
        task_id="start_kafka",
        ssh_conn_id="kafka_ssh",
        command="bash {{ '/home/somyan/kafka_2.13-4.2.0/start-kafka.sh' }}",
        cmd_timeout=60,
    )

    start_kafka


# ─────────────────────────────────────────────
# DAG 2 — CHECK Kafka (runs EVERY MINUTE)
# ─────────────────────────────────────────────
with DAG(
    dag_id="kafka_cluster_check",
    start_date=datetime(2024, 1, 1),
    schedule="*/6 * * * *",      # ✅ Minute-wise
    catchup=False,
    tags=["kafka", "monitor"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
) as kafka_check_dag:

    check_kafka = SSHOperator(
        task_id="check_kafka",
        ssh_conn_id="kafka_ssh",
        command="nc -z localhost 9092",
        cmd_timeout=30,
    )

    check_kafka