# =============================================================================
# 0. !!!ONLY FOR INSTRUCTOR - LAB ASSESTS PROVISIONING CODE!!!!!
# Run the following code in a Python session inside your CAI workbench
# It will create CAI project, database and tables for each user to use in the labs
# Only thing you need to change in this script is the NUM_ATTENDEES value to the
# number of people that will be doing the HOL.
# =============================================================================

import os
import re
import time
import json
import requests
import cmlapi
from cmlapi.utils import Cursor
from cmlapi.models import RotateV1KeyRequest
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, rand
import cml.data_v1 as cmldata

# =============================================================================
# 1. CONFIGURATION / CONTROL PANEL
# =============================================================================
NUM_ATTENDEES = 1
USERNAME = os.getenv("PROJECT_OWNER")
DOMAIN = os.getenv("CDSW_DOMAIN")
GIT_REPO_URL = "https://github.com/richard-vh/cloudera-iceberg-clo-housekeeping-hol.git"
PROJECT_NAME_PATTERN = r"user\d+_hol"
RUNTIME_IMAGE_NAME = "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-jupyterlab-python3.12-standard:2026.04.1-b7"

# Spark Tuning (Bloat Control)
NUM_COMMITS = 30           
RECORDS_PER_COMMIT = 500000 
FILES_PER_COMMIT = 25      
# =============================================================================

# 2. INITIALIZE CLIENT & ROTATE KEY
try:
    client = cmlapi.default_client()
except (ModuleNotFoundError, ValueError):
    cluster_url = os.getenv("CDSW_API_URL", "")
    target = f"{cluster_url[:-1]}2/python.tar.gz"
    os.system(f"pip3 install {target} --quiet")
    import cmlapi
    client = cmlapi.default_client()

def get_fresh_key(api_client, user):
    """Uses the SDK to rotate the V1 key and return the new string."""
    body = RotateV1KeyRequest()
    response = api_client.rotate_v1_key(body, user)
    return response.api_key

API_KEY = get_fresh_key(client, USERNAME)
auth = (API_KEY, "")
v1_base_url = f"https://{DOMAIN}/api/v1"

# 3. DYNAMIC LOOKUPS (Runtime & Connection)
print("--- Starting Dynamic Lookups ---")

# Find Runtime ID
v1_catalog = requests.get(f"{v1_base_url}/runtimes", params={"includeAll": "true"}, auth=auth)
v1_catalog.raise_for_status()
runtimes = v1_catalog.json().get('runtimes', [])
target_runtime_id = next((rt['id'] for rt in runtimes if rt.get('imageIdentifier') == RUNTIME_IMAGE_NAME), None)
print(f"Target Runtime ID: {target_runtime_id}")

# Find Spark Connection Name
conn_url = f"{v1_base_url}/projects/1/data-connections"
conn_resp = requests.get(conn_url, auth=auth)
connections = conn_resp.json().get("projectDataConnectionList", [])
CONNECTION_NAME = next(c["name"] for c in connections if c.get("type") == "SPARK")
print(f"Using Connection: {CONNECTION_NAME}")


# 4. CLEANUP & SPARK INIT
print(f"\n--- Cleaning up existing lab projects ---")
all_projects = Cursor(client.list_projects).items()
project_regex = re.compile(PROJECT_NAME_PATTERN)

for p in all_projects:
    if project_regex.match(p.name):
        try:
            print(f"Deleting: {p.name}")
            client.delete_project(p.id)
            time.sleep(0.5)
        except Exception as e:
            print(f"Error deleting {p.name}: {e}")

print("\n--- Initializing Spark ---")
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.adaptive.enabled", "false")


# 5. PROVISIONING LOOP
print(f"\n--- Provisioning {NUM_ATTENDEES} Attendees ---")

for i in range(1, NUM_ATTENDEES + 1):
    user_suffix = str(i).zfill(3)
    user_name = f"user{user_suffix}"
    project_name = f"{user_name}_hol"
    user_db = f"lakehouse_optimizer_{user_name}"
    
    # Create Project via v1 (Ensures Runtime is attached at birth)
    create_url = f"{v1_base_url}/users/{USERNAME}/projects"
    payload = {
        "name": project_name,
        "project_visibility": "public",
        "template": "git",
        "gitUrl": GIT_REPO_URL,
        "isPrototype": False,
        "supportAsync": True,
        "runtimesEnabled": [int(target_runtime_id)] if target_runtime_id else []
    }

    try:
        response = requests.post(create_url, json=payload, auth=auth)
        if response.status_code in [200, 201]:
            print(f"[{user_name}] CML Project created.")
        else:
            print(f"[{user_name}] Creation failed: {response.text}")

        # Data Generation
        main_table = f"{user_db}.sensor_telemetry"
        orig_table = f"{user_db}.sensor_telemetry_original"
        
        spark.sql(f"DROP TABLE IF EXISTS {main_table} PURGE")
        spark.sql(f"DROP TABLE IF EXISTS {orig_table} PURGE")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {user_db}")
        
        table_schema = """
            (sensor_id STRING, event_time TIMESTAMP, temperature DOUBLE, vibration_freq DOUBLE, status STRING)
            USING iceberg
            PARTITIONED BY (days(event_time))
            TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')
        """
        spark.sql(f"CREATE TABLE {main_table} {table_schema}")
        spark.sql(f"CREATE TABLE {orig_table} {table_schema}")

        for batch in range(NUM_COMMITS):
            df = spark.range(0, RECORDS_PER_COMMIT).select(
                expr("uuid()").alias("sensor_id"),
                expr("current_timestamp() - (INTERVAL '1' DAY * CAST(rand() * 30 AS INT))").alias("event_time"),
                (rand() * 100).alias("temperature"),
                (rand() * 5000).alias("vibration_freq"),
                expr("CASE WHEN rand() > 0.9 THEN 'WARNING' ELSE 'OK' END").alias("status")
            )
            df.repartition(FILES_PER_COMMIT).write.format("iceberg").mode("append").saveAsTable(main_table)
            df.repartition(FILES_PER_COMMIT).write.format("iceberg").mode("append").saveAsTable(orig_table)
            print(f"   [{user_name}] Batch {batch+1}/{NUM_COMMITS} written.")

    except Exception as e:
        print(f"[{user_name}] Provisioning error: {e}")

print("\n🏆 Lab provisioning complete.")
