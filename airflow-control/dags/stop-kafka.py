from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(
    dag_id="kafka_cluster_stop",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kafka"],
) as dag:

    stop_kafka = SSHOperator(
        task_id="stop_kafka",
        ssh_conn_id="kafka_ssh",
        command="bash {{ '/home/somyan/kafka_2.13-4.2.0/stop-kafka.sh' }}",
    )