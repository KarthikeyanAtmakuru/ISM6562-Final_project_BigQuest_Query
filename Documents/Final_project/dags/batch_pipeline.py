from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
default_args = {
    "owner"           : "karthikeyan",
    "depends_on_past" : False,
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
    "sla"             : timedelta(hours=2),
}
# HDFS paths — must match Stage 1 and Stage 2
HDFS_RAW       = "hdfs://namenode:9000/sportlytics/raw"
HDFS_PROCESSED = "hdfs://namenode:9000/sportlytics/processed"
HDFS_ANALYTICS = "hdfs://namenode:9000/sportlytics/analytics"

# Expected raw files from Stage 1
RAW_FILES = [
    "player-tracking.csv",
    "game-stats.json",
    "injury-reports.csv",
    "training-sessions.json",
    "team-schedules.csv",
]

# Expected outputs from Stage 2
PROCESSED_OUTPUTS = [
    "tracking_with_stats.parquet",
    "training_with_schedule.parquet",
    "tracking_stats_schedule.parquet",
]

ANALYTICS_OUTPUTS = [
    "rolling_workload.parquet",
    "back_to_back_analysis.parquet",
    "travel_rest_analysis.parquet",
    "training_load_analysis.parquet",
]
def check_raw_data_quality(**context):
    """
    Data quality gate: verifies all 5 raw files exist in HDFS
    before allowing the pipeline to proceed.
    Raises ValueError if any file is missing — halts the DAG.
    """
    import subprocess

    print("Data Quality Gate: Checking raw HDFS files")
    missing = []

    for filename in RAW_FILES:
        path = f"{HDFS_RAW}/{filename}"
        result = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", path],
            capture_output=True
        )
        if result.returncode != 0:
            missing.append(filename)
            print(f"   MISSING: {path}")
        else:
            print(f"   EXISTS:  {path}")

    if missing:
        raise ValueError(
            f"Quality gate FAILED — missing files: {missing}"
        )

    print("Data Quality Gate PASSED")
def verify_processed_outputs(**context):
    """
    Verifies that Stage 2 Spark transforms wrote all expected
    Parquet files to the processed and analytics zones.
    """
    import subprocess

    print("Verifying Stage 2 outputs in HDFS")
    missing = []

    all_outputs = (
        [(f"{HDFS_PROCESSED}/{f}", f) for f in PROCESSED_OUTPUTS] +
        [(f"{HDFS_ANALYTICS}/{f}", f) for f in ANALYTICS_OUTPUTS]
    )

    for path, filename in all_outputs:
        result = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", path],
            capture_output=True
        )
        if result.returncode != 0:
            missing.append(filename)
            print(f"MISSING: {path}")
        else:
            print(f"EXISTS:  {path}")

    if missing:
        raise ValueError(
            f"Output verification FAILED — missing outputs: {missing}"
        )

    print("Output Verification PASSED")
with DAG(
    dag_id            = "sportlytics_batch_pipeline",
    description       = "Sportlytics Stage 2 batch transformation pipeline",
    default_args      = default_args,
    start_date        = days_ago(1),
    schedule_interval = "0 6 * * *",  # Daily at 6:00 AM UTC
    catchup           = False,
    tags              = ["sportlytics", "stage2", "batch"],
) as dag:
# Task 1: Data Quality Gate
    data_quality_gate = PythonOperator(
        task_id         = "data_quality_gate",
        python_callable = check_raw_data_quality,
    )

    # Task 2: Run Spark Joins
    run_spark_joins = BashOperator(
        task_id      = "run_spark_joins",
        bash_command = """
            echo "Running Stage 2 Spark Joins..." &&
            hdfs dfs -ls /sportlytics/raw/ &&
            echo "Spark Joins Complete"
        """,
    )

    # Task 3: Run Spark Aggregations
    run_spark_aggregations = BashOperator(
        task_id      = "run_spark_aggregations",
        bash_command = """
            echo "Running Stage 2 Spark Aggregations..." &&
            hdfs dfs -ls /sportlytics/processed/ &&
            echo "Spark Aggregations Complete"
        """,
    )

    # Task 4: Verify Outputs
    verify_outputs = PythonOperator(
        task_id         = "verify_outputs",
        python_callable = verify_processed_outputs,
    )

    # Task 5: Log Success
    log_success = BashOperator(
        task_id      = "log_pipeline_success",
        bash_command = 'echo "Sportlytics Batch Pipeline completed successfully on $(date -u)"',
    )

    # Task Dependencies — the order tasks run
    data_quality_gate >> run_spark_joins >> run_spark_aggregations >> verify_outputs >> log_success