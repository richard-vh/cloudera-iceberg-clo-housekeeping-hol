import os
import time
import cml.data_v1 as cmldata
import sys

# 1. Setup Connections
# S3 Client for Physical Scan
S3_CONNECTION_NAME = "S3 Object Store"
conn_s3 = cmldata.get_connection(S3_CONNECTION_NAME)
client = conn_s3.get_base_connection()

# Impala Connection for Metadata
IMPALA_CONNECTION_NAME = "instructor-dm-dh"
conn_impala = cmldata.get_connection(IMPALA_CONNECTION_NAME)

# 2. Parameters
BUCKET = "hol-buk-2cb5568f"

if len(sys.argv) < 3:
    print("Error: Please provide database and table name.")
    print("Example: %run -i get_table_stats2.py my_db my_table")
    sys.exit(1) 
else:
    DB_NAME = sys.argv[1]
    TABLE_NAME = sys.argv[2]
    
    # Dynamically build paths
    # Note: Impala uses backticks for the .files metadata table
    TABLE_PATH = f"{DB_NAME}.{TABLE_NAME}"
    PARENT_PREFIX = f"data/warehouse/tablespace/external/hive/{DB_NAME}.db/{TABLE_NAME}/"


def get_complete_table_stats(bucket, parent_prefix, table_full_path):
    # --- Part A: Get TOTAL Physical Data from S3 ---
    data_prefix = parent_prefix + "data/"
    paginator = client.get_paginator('list_objects_v2')
    
    total_data_files = 0
    total_data_bytes = 0
    
    for page in paginator.paginate(Bucket=bucket, Prefix=data_prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('/'):
                total_data_files += 1
                total_data_bytes += obj['Size']

   
    # --- Part B: Get Snapshot stats from IMPALA ---
    # Using the .files metadata extension in Iceberg/Impala
    query = f"""
        SELECT 
            count(*) as file_count,
            sum(file_size_in_bytes) / (1024*1024) as total_mb,
            avg(file_size_in_bytes) / (1024*1024) as avg_mb
        FROM {table_full_path}.`files`
    """
    
    # Execute via Impala and get result as a Pandas DataFrame
    df_summary = conn_impala.get_pandas_dataframe(query)
    
    # Extract values from the first row of the dataframe
    file_count = df_summary['file_count'].iloc[0]
    total_mb = df_summary['total_mb'].iloc[0]
    avg_mb = df_summary['avg_mb'].iloc[0]

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
    
    print(f"{'total data':<50} | {total_data_files:<8} | {total_data_bytes/(1024**2):<10.2f}")
    
    # Updated to use the variables extracted from Impala
    print(f"{'data (current snapshot)':<50} | {int(file_count):<8} | {total_mb:<10.2f}")
    
    avg_label = "  > average data file size"
    print(f"{avg_label:<50} | {'':<8} | {avg_mb:<10.2f}")
    
    print(f"{'metadata':<50} | {total_meta_files:<8} | {total_meta_mb:<10.2f}")
    for cat, data in meta_stats.items():
        label = f"  > {cat}"
        print(f"{label:<50} | {data[0]:<8} | {data[1]/(1024**2):<10.4f}")

    print("\n")

# Execute
try:
    get_complete_table_stats(BUCKET, PARENT_PREFIX, TABLE_PATH)
finally:
    # Always close the Impala connection
    conn_impala.close()
