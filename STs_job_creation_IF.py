import os
import csv
import google.auth
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# === Helper Function ===
def read_configs_from_csv(file_path):
    """Reads multiple configuration rows from CSV into a list of dicts."""
    with open(file_path, mode="r", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        return [row for row in reader]

# === Project ID ===
PROJECT_ID = "prj-0n-dta-pt-ai-sandbox"

# === Load Configs ===
configs = read_configs_from_csv("config.csv")

# === Get Google Cloud credentials ===
credentials, _ = google.auth.default(
    quota_project_id=PROJECT_ID,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
credentials.refresh(Request())

# === Build the Storage Transfer client ===
storagetransfer = build("storagetransfer", "v1", credentials=credentials)

# === Iterate through all rows in config.csv ===
for idx, config in enumerate(configs, start=1):
    GCS_BUCKET_NAME = config["GCS_BUCKET_NAME"]
    JOB_ID = config["JOB_ID"]
    AZURE_STORAGE_ACCOUNT = config["AZURE_STORAGE_ACCOUNT"]
    AZURE_CONTAINER_NAME = config["AZURE_CONTAINER_NAME"]
    PRIVATE_NETWORK_SERVICE = config.get("PRIVATE_NETWORK_SERVICE", "").strip()
    AZURE_CLIENT_ID = config["AZURE_CLIENT_ID"]       # App registration Client ID
    AZURE_TENANT_ID = config["AZURE_TENANT_ID"]       # Azure Tenant ID
    SOURCE_PREFIX = config.get("SOURCE_PREFIX", "").strip()
    DEST_PREFIX = config.get("DEST_PREFIX", "").strip()
    DESCRIPTION = config.get("DESCRIPTION", f"Transfer Job {idx}")

    CUSTOM_JOB_NAME = f"transferJobs/{JOB_ID}"

    # === Define Transfer Job using Workload Identity Federation ===
    transfer_job = {
        "name": CUSTOM_JOB_NAME,
        "description": f"{DESCRIPTION} | Custom ID: {JOB_ID}",
        "status": "ENABLED",
        "projectId": PROJECT_ID,
        "transferSpec": {
            "azureBlobStorageDataSource": {
                "storageAccount": AZURE_STORAGE_ACCOUNT,
                "container": AZURE_CONTAINER_NAME,
                #  This replaces SAS token credentials
                "federatedIdentityConfig": {
                    "clientId": AZURE_CLIENT_ID,
                    "tenantId": AZURE_TENANT_ID
                },
                # Optional, only include if you use Private Service Connect
                "privateNetworkService": PRIVATE_NETWORK_SERVICE if PRIVATE_NETWORK_SERVICE else None
            },
            "objectConditions": {
                "includePrefixes": [SOURCE_PREFIX] if SOURCE_PREFIX else []
            },
            "gcsDataSink": {
                "bucketName": GCS_BUCKET_NAME,
                "path": DEST_PREFIX if DEST_PREFIX else ""
            },
            "transferOptions": {
                "overwriteObjectsAlreadyExistingInSink": False
            }
        }
    }

    # Remove any None fields to avoid API rejection
    def remove_none(d):
        if isinstance(d, dict):
            return {k: remove_none(v) for k, v in d.items() if v is not None}
        elif isinstance(d, list):
            return [remove_none(v) for v in d if v is not None]
        else:
            return d

    transfer_job = remove_none(transfer_job)

    # === Create Transfer Job ===
    print(f"\nCreating transfer job {idx}: {DESCRIPTION}")
    try:
        response = storagetransfer.transferJobs().create(body=transfer_job).execute()
        print(" Transfer Job Created Successfully:")
        print(f"  Custom Job ID: {JOB_ID}")
        print(f"  API Job Name: {response.get('name')}")
        print(f"  Status: {response.get('status')}")
        print(f"  Source Prefix: {SOURCE_PREFIX}")
        print(f"  Destination Prefix: {DEST_PREFIX}")
    except Exception as e:
        print(f" Failed to create transfer job {idx} ({DESCRIPTION})")
        print(f"  Error: {e}")
