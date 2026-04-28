import os
import time
import cml.data_v1 as cmldata
import sys

# 1. Setup Connections
# S3 Client for Physical Scan (Scanning all files in the bucket)
CONNECTION_NAME = "S3 Object Store"
conn_s3 = cmldata.get_connection(CONNECTION_NAME)
client = conn_s3.get_base_connection()

# Spark Session for Metadata (Querying current snapshot only)
#SPARK_CONNECTION_NAME = "rvh-aw-dl"
#conn_spark = cmldata.get_connection(SPARK_CONNECTION_NAME)
#spark = conn_spark.get_spark_session()
spark.sparkContext.setLogLevel("OFF")

# 2. Parameters
BUCKET = "rvh-buk-f220581f"
#PARENT_PREFIX = "data/warehouse/tablespace/external/hive/lakehouse_optimizer_user001.db/sensor_telemetry/"
#TABLE_PATH = "lakehouse_optimizer_user001.sensor_telemetry"

# Check if arguments were passed
if len(sys.argv) < 3:
    print("Error: Please provide database and table name.")
    print("Example: %run -i get_table_stats2.py my_db my_table")
    # Stop execution if parameters are missing
    sys.exit(1) 
else:
    DB_NAME = sys.argv[1]
    TABLE_NAME = sys.argv[2]
    
#    DB_NAME = "lakehouse_optimizer_user001"
#    TABLE_NAME = "sensor_telemetry"
    
    # Dynamically build paths
    TABLE_PATH = f"{DB_NAME}.{TABLE_NAME}"
    PARENT_PREFIX = f"data/warehouse/tablespace/external/hive/{DB_NAME}.db/{TABLE_NAME}/"


def get_complete_table_stats(bucket, parent_prefix, table_full_path):
    # --- Part A: Get TOTAL Physical Data from S3 (Includes history/bloat) ---
    data_prefix = parent_prefix + "data/"
    paginator = client.get_paginator('list_objects_v2')
    
    total_data_files = 0
    total_data_bytes = 0
    
    for page in paginator.paginate(Bucket=bucket, Prefix=data_prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            # FIX: Ignore directory markers (keys ending in /)
            if not key.endswith('/'):
                total_data_files += 1
                total_data_bytes += obj['Size']

    # --- Part B: Get CURRENT Snapshot stats from Spark ---
    # This reflects the optimized state after compaction
    summary = spark.sql(f"SELECT * FROM {table_full_path}.files").selectExpr(
        "count(*) as file_count",
        "sum(file_size_in_bytes) / (1024*1024) as total_mb",
        "avg(file_size_in_bytes) / (1024*1024) as avg_mb"
    ).collect()[0]

    # --- Part C: Get Metadata folder breakdown from S3 ---
    metadata_prefix = parent_prefix + "metadata/"
    meta_stats = {'manifest': [0, 0], 'snapshot': [0, 0], 'schema': [0, 0]}
    
    for page in paginator.paginate(Bucket=bucket, Prefix=metadata_prefix):
        for obj in page.get('Contents', []):
            fname = os.path.basename(obj['Key'])
            size = obj['Size']
            if fname.endswith('.metadata.json'):
                cat = 'manifest'
            elif fname.startswith('snap-'):
                cat = 'snapshot'
            else:
                cat = 'schema'
            meta_stats[cat][0] += 1
            meta_stats[cat][1] += size

    total_meta_files = sum(d[0] for d in meta_stats.values())
    total_meta_mb = sum(d[1] for d in meta_stats.values()) / (1024**2)

    # --- Part D: Printing the Unified Table ---
    time.sleep(0.5) 
    
    print(f"\n{table_full_path.upper()}\n")
    print(f"{'FOLDER / CATEGORY':<50} | {'FILES':<8} | {'SIZE (MB)':<10}")
    print("-" * 75)
    
    # 1. Total Data (Physical S3 Reality)
    print(f"{'total data':<50} | {total_data_files:<8} | {total_data_bytes/(1024**2):<10.2f}")
    
    # 2. Data (Current Snapshot Logic)
    print(f"{'data (current snapshot)':<50} | {summary['file_count']:<8} | {summary['total_mb']:<10.2f}")
    
    avg_label = "  > average data file size"
    print(f"{avg_label:<50} | {'':<8} | {summary['avg_mb']:<10.2f}")
    
    # 3. Metadata Breakdown
    print(f"{'metadata':<50} | {total_meta_files:<8} | {total_meta_mb:<10.2f}")
    for cat, data in meta_stats.items():
        label = f"  > {cat}"
        print(f"{label:<50} | {data[0]:<8} | {data[1]/(1024**2):<10.4f}")

    print("\n")
# Execute
get_complete_table_stats(BUCKET, PARENT_PREFIX, TABLE_PATH)