# Cloudera Iceberg & Lakehouse Optimizer (CLO) Housekeeping HOL

This repository contains the materials and scripts for a comprehensive Hands-On Lab (HOL) focused on managing and optimizing **Apache Iceberg** tables within the Cloudera Data Platform. 

The lab guides users through the transition from manual "housekeeping" tasks to intelligent, automated optimization using the **Cloudera Lakehouse Optimizer (CLO)**.

## 📂 Project Structure

* **`lab0-before-you-start.ipynb`**: Introduction to the Hand-on Lab enviornment.
* **`lab1-introduction-iceberg-with-spark.ipynb`**: Covers Iceberg fundamentals, including table creation, ACID transactions, schema/partition evolution, and time travel.
* **`lab2-iceberg-housekeeping-hol.ipynb`**: The primary lab notebook covering manual compaction, snapshot expiration, and the configuration of automated CLO policies.
* **`get_table_stats.py`**: A utility script used to perform physical S3 scans and logical Spark audits to identify "Small File Syndrome" and metadata bloat.
* **`assets/create_user_assets.py`**: A provisioning script used to generate "bloated" tables for users and CAI projects for working in.
* **`assets/`**: Contains images used throughout the lab documentation.

## 🚀 Lab Overview

### Lab 1: Iceberg fundamentals
The lab begins with a recap of Apache Iceberg fundamentals.

### Lab 2: Manual Housekeeping
Users learn the technical "why" behind table maintenance:
* **Identifying Bloat**: Using `get_table_stats.py` to find tables with high file counts and tiny average file sizes.
* **Manual Compaction**: Running `CALL system.rewrite_data_files` to merge small Parquet files into optimized 128MB chunks.
* **Benchmarking**: Proving performance gains by comparing execution times between bloated and optimized tables.
* **Storage Reclamation**: Manually expiring snapshots to physically delete orphaned data files from S3.

### Lab 3: Automated Optimization with CLO
The second half introduces the **Cloudera Lakehouse Optimizer (CLO)**, a "set-and-forget" service:
* **Intelligent Policies**: Configuring automated schedules and maintenance rules based on table health metrics.
* **Scale**: Transitioning from manual code for every table to managing thousands of tables via a single interface.
* **Observability**: Monitoring policy execution and table health through the CLO dashboard and granular table-level views.

## 📊 Results & Impact
By completing this lab, users demonstrate how to eliminate "Small File Syndrome" and maintain a high-performance, cost-effective Lakehouse that is always "AI-Ready".

---
*Developed for Cloudera Hands-On Learning.*
