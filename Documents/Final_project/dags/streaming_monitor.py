from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
default_args = {
    "owner"           : "karthikeyan",
    "depends_on_past" : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=2),
    "sla"             : timedelta(minutes=15),
}

# Paths and config — must match Stage 3
KAFKA_BROKER    = "kafka:9092"
KAFKA_TOPIC     = "realtime_player_telemetry"
ALERTS_PATH     = "hdfs://namenode:9000/sportlytics/analytics/streaming_alerts.parquet"
CHECKPOINT_PATH = "hdfs://namenode:9000/sportlytics/checkpoints/streaming/fatigue_alerts"
def check_kafka_broker(**context):
    """
    Verifies Kafka broker is reachable.
    Raises an error if the broker is down.
    """
    import subprocess

    print("Checking Kafka Broker Health")
    result = subprocess.run(
        [
            "kafka-broker-api-versions",
            "--bootstrap-server", KAFKA_BROKER
        ],
        capture_output=True,
        text=True,
        timeout=15
    )

    if result.returncode != 0:
        raise ConnectionError(
            f"Kafka broker at {KAFKA_BROKER} is NOT reachable.\n"
            f"Error: {result.stderr}"
        )

    print(f"Kafka broker at {KAFKA_BROKER} is healthy")
    print("Kafka Broker Check PASSED")

def check_streaming_checkpoint(**context):
    """
    Verifies that the Spark Structured Streaming checkpoint
    directory exists in HDFS, proving the streaming job has run.
    """
    import subprocess

    print(f"Checking Streaming Checkpoint")
    result = subprocess.run(
        ["hdfs", "dfs", "-test", "-d", CHECKPOINT_PATH],
        capture_output=True
    )

    if result.returncode != 0:
        raise FileNotFoundError(
            f"Checkpoint directory not found at {CHECKPOINT_PATH}.\n"
            f"This means Stage 3 streaming job has not run yet."
        )

    print(f"Checkpoint exists at {CHECKPOINT_PATH}")
    print("Checkpoint Check PASSED")


def check_alerts_output(**context):
    """
    Verifies fatigue alert records have been written to HDFS.
    """
    import subprocess

    print(f"Checking Streaming Alerts Output")
    result = subprocess.run(
        ["hdfs", "dfs", "-test", "-e", ALERTS_PATH],
        capture_output=True
    )

    if result.returncode != 0:
        print(
            f"WARNING: No alerts written to {ALERTS_PATH} yet.\n"
            f"This is expected if no players exceeded fatigue thresholds."
        )
        return

    print(f"Streaming alerts exist at {ALERTS_PATH}")
    print("Alerts Output Check PASSED")
with DAG(
    dag_id            = "sportlytics_streaming_monitor",
    description       = "Health monitor for Sportlytics Stage 3 streaming pipeline",
    default_args      = default_args,
    start_date        = days_ago(1),
    schedule_interval = "*/30 18-23 * * *",  # Every 30 min, 6PM-midnight
    catchup           = False,
    tags              = ["sportlytics", "stage3", "streaming", "monitoring"],
) as dag:

    # Task 1: Check Kafka Broker
    check_broker = PythonOperator(
        task_id         = "check_kafka_broker",
        python_callable = check_kafka_broker,
    )

    # Task 2: Check Kafka Topic
    check_topic = BashOperator(
        task_id      = "check_kafka_topic",
        bash_command = f"""
            kafka-topics --bootstrap-server {KAFKA_BROKER} \
            --describe --topic {KAFKA_TOPIC} &&
            echo "Topic {KAFKA_TOPIC} exists and is healthy"
        """,
    )

    # Task 3: Check Streaming Checkpoint
    check_checkpoint = PythonOperator(
        task_id         = "check_streaming_checkpoint",
        python_callable = check_streaming_checkpoint,
    )

    # Task 4: Check Alerts Output
    check_alerts = PythonOperator(
        task_id         = "check_alerts_output",
        python_callable = check_alerts_output,
    )

    # Task 5: Report Status
    report_status = BashOperator(
        task_id      = "report_streaming_status",
        bash_command = 'echo "Streaming pipeline health check PASSED on $(date -u)"',
    )

    # Task Dependencies
    check_broker >> check_topic >> check_checkpoint >> check_alerts >> report_status