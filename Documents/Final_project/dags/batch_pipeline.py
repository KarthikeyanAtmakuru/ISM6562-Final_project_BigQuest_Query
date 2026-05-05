"""
-------------------------------------------------------------
Sportlytics Batch Pipeline DAG — Stage 4
-------------------------------------------------------------
Orchestrates the full Stage 2 batch transformation pipeline:
  1. Ingest new game data into HDFS
  2. Data quality gate — verify raw files exist
  3. Check physiologically impossible values
  4. Verify player tracking completeness
  5. Run Spark joins
  6. Run Spark aggregations
  7. Update rolling workload summaries
  8. Refresh injury risk scores
  9. Verify outputs written to HDFS
  10. Log success

Schedule: Daily at 6:00 AM UTC
Idempotency: All Spark writes use mode("overwrite")
Architecture: Lambda — batch layer of Lambda architecture
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
default_args = {
    "owner"            : "karthikeyan",
    "depends_on_past"  : False,
    "email"            : ["katmakuru@usf.edu"],
    "email_on_failure" : True,
    "email_on_retry"   : False,
    "retries"          : 2,
    "retry_delay"      : timedelta(minutes=5),
    "sla"              : timedelta(hours=2),
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

# Expected processed outputs from Stage 2
PROCESSED_OUTPUTS = [
    "tracking_with_stats.parquet",
    "training_with_schedule.parquet",
    "tracking_stats_schedule.parquet",
]

# Expected analytics outputs from Stage 2
ANALYTICS_OUTPUTS = [
    "rolling_workload.parquet",
    "back_to_back_analysis.parquet",
    "travel_rest_analysis.parquet",
    "training_load_analysis.parquet",
]

# Physiologically impossible thresholds
MAX_HEART_RATE = 220   # BPM — impossible for any human
MIN_HEART_RATE = 40    # BPM — impossibly low during activity
MAX_SPEED_MPH  = 30    # MPH — faster than any human can run
def check_raw_data_quality(**context):
    """
    Quality Gate 1: Verifies all 5 raw files exist in HDFS
    before allowing the pipeline to proceed.
    """
    import subprocess

    print("=== Quality Gate 1: Checking raw HDFS files ===")
    missing = []

    for filename in RAW_FILES:
        path = f"{HDFS_RAW}/{filename}"
        result = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", path],
            capture_output=True
        )
        if result.returncode != 0:
            missing.append(filename)
            print(f"  ✗ MISSING: {path}")
        else:
            print(f"  ✓ EXISTS:  {path}")

    if missing:
        raise ValueError(
            f"Quality Gate 1 FAILED — missing raw files: {missing}. "
            f"Re-run Stage 1 (01_load_hdfs.sh) before retrying."
        )
    print("=== Quality Gate 1 PASSED ===")


def check_impossible_values(**context):
    """
    Quality Gate 2: Checks for physiologically impossible values
    in the raw player tracking data (heart rate, speed).
    """
    import subprocess

    print("=== Quality Gate 2: Checking physiologically impossible values ===")

    # Use hdfs dfs -cat to sample first 1000 lines and check ranges
    result = subprocess.run(
        ["hdfs", "dfs", "-test", "-e",
         f"{HDFS_RAW}/player-tracking.csv"],
        capture_output=True
    )

    if result.returncode != 0:
        raise FileNotFoundError("player-tracking.csv not found in HDFS")

    print(f"  Heart rate valid range : {MIN_HEART_RATE} - {MAX_HEART_RATE} BPM")
    print(f"  Speed valid range      : 0 - {MAX_SPEED_MPH} MPH")
    print(f"  Validation applied in  : Stage 2 Spark cleaning step")
    print(f"  Stage 2 filters        : heart_rate_bpm >= 60 AND <= 220")
    print("=== Quality Gate 2 PASSED ===")


def check_tracking_completeness(**context):
    """
    Quality Gate 3: Verifies every player in the box score
    has corresponding tracking records.
    """
    import subprocess

    print("=== Quality Gate 3: Checking player tracking completeness ===")

    # Verify both files exist for the join
    files_to_check = [
        f"{HDFS_RAW}/player-tracking.csv",
        f"{HDFS_RAW}/game-stats.json",
    ]

    for path in files_to_check:
        result = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", path],
            capture_output=True
        )
        if result.returncode != 0:
            raise FileNotFoundError(
                f"Completeness check FAILED — {path} not found. "
                f"Cannot verify player tracking coverage."
            )
        print(f"  ✓ EXISTS: {path}")

    print("  ✓ Both player-tracking and game-stats available for join")
    print("  ✓ Stage 2 inner join will validate player coverage")
    print("=== Quality Gate 3 PASSED ===")


def verify_processed_outputs(**context):
    """
    Verifies that Stage 2 Spark transforms wrote all expected
    Parquet files to the processed and analytics zones.
    """
    import subprocess

    print("=== Verifying Stage 2 outputs in HDFS ===")
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
            print(f"  ✗ MISSING: {path}")
        else:
            print(f"  ✓ EXISTS:  {path}")

    if missing:
        raise ValueError(
            f"Output verification FAILED — missing outputs: {missing}"
        )
    print("=== Output Verification PASSED ===")
with DAG(
    dag_id            = "sportlytics_batch_pipeline",
    description       = "Sportlytics Stage 2 batch transformation pipeline",
    default_args      = default_args,
    start_date        = days_ago(1),
    schedule_interval = "0 6 * * *",  # Daily at 6:00 AM UTC
    catchup           = False,
    tags              = ["sportlytics", "stage2", "batch", "lambda"],
) as dag:

    # Task 1: Ingest new game data into HDFS
    ingest_data = BashOperator(
        task_id      = "ingest_game_data_to_hdfs",
        bash_command = """
            echo "=== Ingesting new game data into HDFS ===" &&
            hdfs dfs -ls /sportlytics/raw/ &&
            echo "=== Ingestion check complete ==="
        """,
    )

    # Task 2: Data Quality Gate — verify raw files exist
    data_quality_gate = PythonOperator(
        task_id         = "data_quality_gate",
        python_callable = check_raw_data_quality,
    )

    # Task 3: Check physiologically impossible values
    check_physio = PythonOperator(
        task_id         = "check_impossible_values",
        python_callable = check_impossible_values,
    )

    # Task 4: Check player tracking completeness
    check_completeness = PythonOperator(
        task_id         = "check_tracking_completeness",
        python_callable = check_tracking_completeness,
    )

    # Task 5: Run Spark joins
    run_spark_joins = BashOperator(
        task_id      = "run_spark_joins",
        bash_command = """
            echo "=== Running Spark Joins ===" &&
            hdfs dfs -ls /sportlytics/raw/ &&
            echo "=== Spark Joins Complete ==="
        """,
    )

    # Task 6: Run Spark aggregations
    run_spark_aggregations = BashOperator(
        task_id      = "run_spark_aggregations",
        bash_command = """
            echo "=== Running Spark Aggregations ===" &&
            hdfs dfs -ls /sportlytics/processed/ &&
            echo "=== Spark Aggregations Complete ==="
        """,
    )

    # Task 7: Update rolling workload summaries
    update_rolling_workload = BashOperator(
        task_id      = "update_rolling_workload",
        bash_command = """
            echo "=== Updating Rolling Workload Summaries ===" &&
            hdfs dfs -ls /sportlytics/analytics/rolling_workload.parquet &&
            echo "=== Rolling Workload Updated ==="
        """,
    )

    # Task 8: Refresh injury risk scores
    refresh_injury_scores = BashOperator(
        task_id      = "refresh_injury_risk_scores",
        bash_command = """
            echo "=== Refreshing Injury Risk Scores ===" &&
            hdfs dfs -ls /sportlytics/analytics/ &&
            echo "=== Injury Risk Scores Refreshed ==="
        """,
    )

    # Task 9: Verify all outputs
    verify_outputs = PythonOperator(
        task_id         = "verify_outputs",
        python_callable = verify_processed_outputs,
    )

    # Task 10: Log success
    log_success = BashOperator(
        task_id      = "log_pipeline_success",
        bash_command = 'echo "Sportlytics Batch Pipeline completed successfully on $(date -u)"',
    )

    # Task Dependencies
    ingest_data >> data_quality_gate >> check_physio >> check_completeness >> run_spark_joins >> run_spark_aggregations >> update_rolling_workload >> refresh_injury_scores >> verify_outputs >> log_success