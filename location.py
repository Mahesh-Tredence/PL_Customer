from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage_transfer
import gcsfs
import json
import re
import os

# --- Automatically get the DAG file name (without .py) ---
DAG_FILE_NAME = os.path.basename(__file__)  # e.g., 'xyz.py'
CONFIG_JSON_FILE = f"{os.path.splitext(DAG_FILE_NAME)[0]}.json"  # 'xyz.json'

# --- Path to the JSON config file in your bucket ---
CONFIG_JSON_PATH = f"gs://us-east4-composer1006-e4e58a2e-bucket/config/{CONFIG_JSON_FILE}"

# --- Load config dynamically from GCS ---
def load_dag_config(json_gcs_path: str):
    fs = gcsfs.GCSFileSystem()
    if not fs.exists(json_gcs_path):
        raise FileNotFoundError(f"Config file not found at: {json_gcs_path}")
    
    with fs.open(json_gcs_path) as f:
        config = json.load(f)
    
    # If JSON is a list, take the first element
    if isinstance(config, list):
        config = config[0]
    
    # Convert start_date string to datetime
    start_date_str = config.get("START_DATE", "")
    try:
        start_date = datetime.strptime(start_date_str, "%m/%d/%Y")
    except ValueError:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    
    # Ensure TRANSFER_JOBS is a list
    transfer_jobs = config.get("TRANSFER_JOBS", [])
    if isinstance(transfer_jobs, str):
        transfer_jobs = [transfer_jobs]
    
    # Ensure TAGS is a list
    tags = config.get("TAGS", [])
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",")]
    
    # Convert "NONE" or empty schedule_interval to Python None
    schedule_interval = config.get("SCHEDULE_INTERVAL", None)
    if schedule_interval in ("", "NONE", None):
        schedule_interval = None

    return {
        "PROJECT_ID": config["PROJECT_ID"],
        "TRANSFER_JOBS": transfer_jobs,
        "DAG_ID": config["DAG_ID"],
        "DESCRIPTION": config.get("DESCRIPTION", ""),
        "SCHEDULE_INTERVAL": schedule_interval,
        "START_DATE": start_date,
        "TAGS": tags,
    }

# --- Load configuration ---
CONFIG = load_dag_config(CONFIG_JSON_PATH)

# --- Function to trigger GCS Transfer jobs ---
def trigger_transfer_job(job_name: str, **context):
    """Trigger an existing GCS Transfer Service job."""
    client = storage_transfer.StorageTransferServiceClient()
    response = client.run_transfer_job(
        request={
            "project_id": CONFIG["PROJECT_ID"],
            "job_name": job_name,
        }
    )
    context['ti'].log.info(f"Triggered job: {job_name}, operation: {response.operation.name}")

# --- Default DAG args ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- Define DAG dynamically ---
with DAG(
    dag_id=CONFIG["DAG_ID"],
    default_args=default_args,
    description=CONFIG["DESCRIPTION"],
    schedule_interval=CONFIG["SCHEDULE_INTERVAL"],  # None if externally triggered
    start_date=CONFIG["START_DATE"],
    catchup=False,
    tags=CONFIG["TAGS"],
) as dag:

    for job_name in CONFIG["TRANSFER_JOBS"]:
        # Sanitize task_id (replace invalid characters)
        safe_task_id = re.sub(r'[^a-zA-Z0-9_]', '_', job_name.split('/')[-1])
        PythonOperator(
            task_id=f"trigger_{safe_task_id}",
            python_callable=trigger_transfer_job,
            op_args=[job_name],
        )
