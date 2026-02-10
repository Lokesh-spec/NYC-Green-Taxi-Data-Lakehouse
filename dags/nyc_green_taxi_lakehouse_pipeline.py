import subprocess
import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from pendulum import datetime

import yaml
from pathlib import Path


local_tz = pendulum.timezone("Asia/Kolkata")
# Load Config
CONFIG_PATH = Path("/opt/airflow/dags/config/pipeline_config.yaml")

with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)


# Helper
def read_sql(path):
    with open(path) as f:
        return f.read()


# Config variables
GCS_BUCKET = config["gcs"]["bucket"]
GCS_BASE_PREFIX = config["gcs"]["base_prefix"]
GCS_LOOKUP_FILE_PATH = config["gcs"]["lookup_file_path"]

BEAM_SCRIPT_PATH = config["beam"]["script_path"]

PROJECT_ID = config["gcp"]["project_id"]
REGION = config["gcp"]["region"]

TEMP_LOCATION = config["dataflow"]["temp_location"]
STAGING_LOCATION = config["dataflow"]["staging_location"]


# DAG
@dag(
    dag_id="nyc_green_taxi_lakehouse_pipeline",
    start_date=datetime(2026, 2, 9, 12, tz=local_tz),
    schedule="@hourly",
    catchup=True,
    tags=["gcs", "beam", "bigquery"]
)
def nyc_green_taxi_lakehouse_pipeline():

    # Task 1: Find parquet files
    @task
    def find_parquet_files(**context) -> str:
        ts = context["data_interval_start"]

        prefix = (
            f"{GCS_BASE_PREFIX}/"
            f"{ts.strftime('%Y')}/"
            f"{ts.strftime('%m')}/"
            f"{ts.strftime('%d')}/"
            f"{ts.strftime('%H')}/"
        )

        gcs_hook = GCSHook()
        objects = gcs_hook.list(bucket_name=GCS_BUCKET, prefix=prefix)

        parquet_files = [o for o in objects if o.endswith(".parquet")]

        if not parquet_files:
            raise AirflowSkipException(
                f"No parquet files found in gs://{GCS_BUCKET}/{prefix}"
            )

        input_pattern = f"gs://{GCS_BUCKET}/{prefix}*.parquet"

        print(f"Found {len(parquet_files)} files")
        return input_pattern


    # Task 2: Run Beam → Bronze
    @task
    def run_beam_pipeline(input_pattern: str, **context):
        if not input_pattern:
            raise AirflowSkipException("No input pattern")

        ts = context["data_interval_start"]
        job_name = f"nyc-green-taxi-bronze-{ts.strftime('%Y%m%d%H')}"

        cmd = [
            "python3",
            BEAM_SCRIPT_PATH,
            "--project", PROJECT_ID,
            "--region", REGION,
            "--input_parquet", input_pattern,
            "--output_table", config["bigquery"]["bronze_trip_table"],
            "--temp_location", TEMP_LOCATION,
            "--staging_location", STAGING_LOCATION,
            "--job_name", job_name,
            "--runner", "DataflowRunner"  # Change to DataflowRunner for actual runs,
        ]

        print(" ".join(cmd))

        print("Running command:", " ".join(cmd))
    
        result = subprocess.run(cmd, capture_output=True, text=True)
        print("STDOUT:\n", result.stdout)
        print("STDERR:\n", result.stderr)
        subprocess.run(cmd, check=True)


    # Task 3: Lookup → Bronze
    def gcs_to_bigquery_lookup_op():
        return GCSToBigQueryOperator(
            task_id="gcs_to_bigquery_lookup",
            bucket=GCS_BUCKET,
            source_objects=[GCS_LOOKUP_FILE_PATH],
            destination_project_dataset_table=config["bigquery"]["bronze_lookup_table"],
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
        )


    # Bronze → Silver
    def bronze_to_silver_trips_op():
        return BigQueryInsertJobOperator(
            task_id="bronze_to_silver_trips",
            configuration={
                "query": {
                    "query": read_sql(config["sql"]["silver"]["trips"]),
                    "useLegacySql": False,
                }
            },
            params={
                "project_id": config["gcp"]["project_id"],
                "bronze_dataset": config["datasets"]["bronze"],
                "silver_dataset": config["datasets"]["silver"],
            },
        )

    def bronze_to_silver_lookup_op():
        return BigQueryInsertJobOperator(
            task_id="bronze_to_silver_lookup",
            configuration={
                "query": {
                    "query": read_sql(config["sql"]["silver"]["lookup"]),
                    "useLegacySql": False,
                }
            },
            params={
                "project_id": config["gcp"]["project_id"],
                "bronze_dataset": config["datasets"]["bronze"],
                "silver_dataset": config["datasets"]["silver"],
            },
        )


    # Gold Dimensions
    def gold_dim_date_op():
        return BigQueryInsertJobOperator(
            task_id="gold_dim_date",
            configuration={
                "query": {
                    "query": read_sql(config["sql"]["gold"]["dim_dates"]),
                    "useLegacySql": False,
                }
            },
            params={
                "project_id": config["gcp"]["project_id"],
                "silver_dataset": config["datasets"]["silver"],
                "gold_dataset": config["datasets"]["gold"],
            },
        )

    def gold_dim_location_op():
        return BigQueryInsertJobOperator(
            task_id="gold_dim_location",
            configuration={
                "query": {
                    "query": read_sql(config["sql"]["gold"]["dim_locations"]),
                    "useLegacySql": False,
                }
            },
            params={
                "project_id": config["gcp"]["project_id"],
                "silver_dataset": config["datasets"]["silver"],
                "gold_dataset": config["datasets"]["gold"],
            },
        )

    def gold_dim_payments_op():
        return BigQueryInsertJobOperator(
            task_id="gold_dim_payments",
            configuration={
                "query": {
                    "query": read_sql(config["sql"]["gold"]["dim_payments"]),
                    "useLegacySql": False,
                }
            },
            params={
                "project_id": config["gcp"]["project_id"],
                "silver_dataset": config["datasets"]["silver"],
                "gold_dataset": config["datasets"]["gold"],
            },
        )


    # Gold Fact
    def gold_fact_trips_op():
        return BigQueryInsertJobOperator(
            task_id="gold_fact_trips",
            configuration={
                "query": {
                    "query": read_sql(config["sql"]["gold"]["fact_trips"]),
                    "useLegacySql": False,
                }
            },
            params={
                "project_id": config["gcp"]["project_id"],
                "silver_dataset": config["datasets"]["silver"],
                "gold_dataset": config["datasets"]["gold"],
            },
        )


    # Analytics
    def analytics_zone_daily_metrics_op():
        return BigQueryInsertJobOperator(
            task_id="analytics_zone_daily_metrics",
            configuration={
                "query": {
                    "query": read_sql(config["sql"]["analytics"]["daily"]),
                    "useLegacySql": False,
                }
            },
            params={
                "project_id": config["gcp"]["project_id"],
                "gold_dataset": config["datasets"]["gold"]
            },
        )

    def analytics_zone_monthly_metrics_op():
        return BigQueryInsertJobOperator(
            task_id="analytics_zone_monthly_metrics",
            configuration={
                "query": {
                    "query": read_sql(config["sql"]["analytics"]["monthly"]),
                    "useLegacySql": False,
                }
            },
            params={
                "project_id": config["gcp"]["project_id"],
                "gold_dataset": config["datasets"]["gold"]
            },
        )


    # Build tasks
    input_pattern = find_parquet_files()
    run_beam_op = run_beam_pipeline(input_pattern)
    gcs_lookup_op = gcs_to_bigquery_lookup_op()

    bronze_silver_trips = bronze_to_silver_trips_op()
    bronze_silver_lookup = bronze_to_silver_lookup_op()

    gold_dates = gold_dim_date_op()
    gold_locations = gold_dim_location_op()
    gold_payments = gold_dim_payments_op()

    gold_trips = gold_fact_trips_op()

    analytics_daily = analytics_zone_daily_metrics_op()
    analytics_monthly = analytics_zone_monthly_metrics_op()


    # DAG wiring
    input_pattern >> [run_beam_op, gcs_lookup_op]

    run_beam_op >> bronze_silver_trips
    gcs_lookup_op >> bronze_silver_lookup

    bronze_silver_trips >> [gold_dates, gold_locations, gold_payments]
    bronze_silver_lookup >> gold_locations

    [gold_dates, gold_locations, gold_payments] >> gold_trips

    gold_trips >> [analytics_daily, analytics_monthly]


dag = nyc_green_taxi_lakehouse_pipeline()
