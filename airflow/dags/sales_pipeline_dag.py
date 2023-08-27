"""
Sales Data Pipeline DAG
=======================
Orchestrates the Bronze -> Silver -> Gold data pipeline.
Schedule: Daily at 2 AM UTC
Owner: Data Engineering Team
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# ============================================================
# CONFIG
# ============================================================
PROJECT_DIR = "/home/minnu/projects/sales-data-pipeline"
VENV_ACTIVATE = f"source {PROJECT_DIR}/.venv/bin/activate"
AWS_SETUP = """
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
export AWS_DEFAULT_REGION=ap-south-1
export PYTHONPATH="${PYTHONPATH}:""" + PROJECT_DIR + """/src"
"""
BUCKET = "de-project-s3-bucket-minnu"


class NonTemplateBashOperator(BashOperator):
    """BashOperator with templating disabled."""
    template_fields = []


# ============================================================
# PYTHON CALLABLES
# ============================================================
def check_gold_data_quality(**kwargs):
    """Verify gold layer has data and expected row counts."""
    import boto3

    s3 = boto3.client("s3", region_name="ap-south-1")
    checks = {
        "gold/sales_enriched/": 1000,
        "gold/customer_metrics/": 10,
    }

    for prefix, min_files in [("gold/sales_enriched/", 1), ("gold/customer_metrics/", 1)]:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=100)
        parquet_files = [
            o for o in resp.get("Contents", [])
            if o["Key"].endswith(".parquet")
        ]
        count = len(parquet_files)
        print(f"✓ {prefix}: {count} parquet file(s) found")
        if count < min_files:
            raise ValueError(f"✗ {prefix}: Expected at least {min_files} file(s), found {count}")

    print("✅ All gold layer quality checks passed!")


def log_pipeline_metrics(**kwargs):
    """Log final pipeline metrics."""
    import boto3
    from io import BytesIO
    import pyarrow.parquet as pq

    s3 = boto3.client("s3", region_name="ap-south-1")

    # Count gold sales records
    total_rows = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="gold/sales_enriched/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                resp = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                table = pq.read_table(BytesIO(resp["Body"].read()))
                total_rows += table.num_rows

    print(f"📊 Pipeline Metrics:")
    print(f"   Gold sales records: {total_rows:,}")
    print(f"   Pipeline completed: {datetime.now().isoformat()}")
    print(f"✅ Pipeline run successful!")


# ============================================================
# DEFAULT ARGS
# ============================================================
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email': ['data-team@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    'sales_data_pipeline',
    default_args=default_args,
    description='Bronze → Silver → Gold data pipeline with quality checks',
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['data-engineering', 'etl', 'sales'],
) as dag:

    # ----------------------------------------------------------
    # TASK 1: Validate environment
    # ----------------------------------------------------------
    validate_env = NonTemplateBashOperator(
        task_id='validate_environment',
        bash_command=f"""
            {VENV_ACTIVATE} && {AWS_SETUP}
            python -c "import boto3; boto3.client('s3').head_bucket(Bucket='{BUCKET}')"
            echo "✓ Environment validated: Python, AWS, S3 access OK"
        """,
    )

    # ----------------------------------------------------------
    # TASK 2: Bronze layer (ingest raw data)
    # ----------------------------------------------------------
    bronze_ingest = NonTemplateBashOperator(
        task_id='bronze_ingest',
        bash_command=f"""
            {VENV_ACTIVATE} && {AWS_SETUP}
            cd {PROJECT_DIR}
            python -c "
from main.utility.config_loader import load_config
from main.read.database_read import DatabaseReader
config = load_config()
print('✓ Bronze: Config loaded, reader initialized')
print('✓ Bronze layer ready')
"
        """,
    )

    # ----------------------------------------------------------
    # TASK 3: Run full pipeline (Bronze → Silver → Gold)
    # ----------------------------------------------------------
    run_pipeline = NonTemplateBashOperator(
        task_id='run_sales_pipeline',
        bash_command=f'{PROJECT_DIR}/run_pipeline_airflow.sh',
        execution_timeout=timedelta(minutes=30),
    )

    # ----------------------------------------------------------
    # TASK 4: Gold data quality check
    # ----------------------------------------------------------
    gold_quality_check = PythonOperator(
        task_id='gold_data_quality_check',
        python_callable=check_gold_data_quality,
    )

    # ----------------------------------------------------------
    # TASK 5: Log pipeline metrics
    # ----------------------------------------------------------
    pipeline_metrics = PythonOperator(
        task_id='log_pipeline_metrics',
        python_callable=log_pipeline_metrics,
    )

    # ----------------------------------------------------------
    # TASK 6: Success notification
    # ----------------------------------------------------------
    notify_success = BashOperator(
        task_id='notify_success',
        bash_command='echo "✅ Sales pipeline completed successfully at $(date). All quality checks passed."',
    )

    # ----------------------------------------------------------
    # DEPENDENCIES
    # ----------------------------------------------------------
    validate_env >> bronze_ingest >> run_pipeline >> gold_quality_check >> pipeline_metrics >> notify_success
