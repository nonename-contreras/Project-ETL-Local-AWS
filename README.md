🚀 Hybrid On-Premise/Cloud ETL: Medallion Architecture

📌 Project Overview

This project is an end-to-end Data Engineering pipeline that utilizes a Hybrid Cloud Strategy. It implements a Medallion Architecture (Bronze, Silver, Gold) to extract, process, and model live data from public APIs (CoinGecko and GitHub).

To optimize infrastructure costs (FinOps), all compute workloads are handled 100% on-premise using a local server (Lanix), while storage and serving layers are delegated to AWS. The entire workflow is orchestrated with Apache Airflow, running in an isolated Dockerized environment.

🏗️ Architecture & Data Flow

```mermaid
graph TD
    subgraph External [External Sources / APIs]
        A[CoinGecko & GitHub]
    end

    subgraph OnPrem [LANIX ON-PREMISE: Ubuntu Server + Docker Compose]
        B[Apache Airflow 2.7.1<br>LocalExecutor] <--> C[(Postgres 15<br>Internal Metadata)]
        B -->|uv manager| D[Workspace: /home/data<br>DAGs, PySpark, dbt]
    end

    subgraph AWS [AWS CLOUD]
        E[(AWS S3 Data Lake<br>Bronze & Silver Layers)]
        F[(AWS RDS PostgreSQL<br>Gold Layer / Data Warehouse)]
    end

    A -->|dlt & requests| B
    D -->|boto3 / PySpark| E
    E -->|JDBC Batching| F
    D -->|dbt models| F ```


🛠️ Tech Stack Components

1. Local Server & Orchestration (Lanix On-Premise)

Base System: Ubuntu Server with secure user workspace isolation.

Orchestration: Apache Airflow 2.7.1 using LocalExecutor for concurrent, non-blocking task execution.

Metadata Database: PostgreSQL 15 (Internal Docker container) dedicated exclusively to Airflow's internal operations (task history, state management).

Package Manager: uv (Rust-based) integrated via multi-stage builds to drastically speed up Python dependency resolution.

2. Processing & Transformation (Python / SQL)

Ingestion: dlt (data load tool) and requests for lightweight, structured extraction from HTTP/REST endpoints.

Batch Processing: PySpark 3.4.1 (Java 11 Headless) handles in-memory transformations, schema validation, and deduplication locally.

Analytical Modeling: dbt-postgres for modular, version-controlled SQL transformations executed directly in the target database (RDS).

3. Cloud Layer (AWS)

Data Lake (Storage): AWS S3 hosts the Bronze (Raw Data) and Silver (Clean/Parquet Data) layers.

Data Warehouse (Serving): AWS RDS (PostgreSQL) manages the refined Gold layer, ready for BI tool consumption (PowerBI/Tableau).

Security: Airflow Connections manage AWS credentials directly from the web UI, eliminating plaintext keys in the source code.

🐳 Docker Deployment

The core of the on-premise orchestration relies on a containerized architecture using Docker Compose. This allows for the centralization and isolation of services without polluting the host physical server:

Core Containers: Dedicated instances are spun up for the Airflow Webserver, the Scheduler, and the PostgreSQL 15 database (exclusive for Airflow metadata).

Volume Mapping: User directories on the local host (/home/data) are mapped directly into the container. This allows DAGs, PySpark scripts, and dbt models to be developed and modified in real-time without the need to rebuild Docker images.

Efficient Builds: The custom Airflow image utilizes multi-stage builds and the uv dependency manager, achieving extremely fast deployment times even when installing heavy libraries (like PySpark and dbt-postgres).

🔄 Execution Plan (Pipeline Phases)

🥉 Phase 1: Extraction (Batch Ingestion - Bronze Layer)

Airflow executes a Python script on the Lanix server to query the external APIs. The JSON response is validated and immediately uploaded to AWS S3 (Bronze Layer) using secure Airflow Hooks.

Cost Optimization: The AWS compute cost for extraction is $0.00, as everything is processed on the on-premise Lanix server.

🥈 Phase 2: Processing & Normalization (Silver Layer)

PySpark reads the raw data from S3 without downloading it physically to the hard drive, applying transformations, null validation, and deduplication within the local server's RAM. The clean data is sent back to S3 in an optimized Parquet format, using logical partitioning by date (fecha=YYYY-MM-DD) and the coalesce(1) function to avoid the Small Files Problem.

🥇 Phase 3: Loading & Modeling (Gold Layer)

The clean data from the Silver layer is ingested into the Staging table on AWS RDS via bulk JDBC writes. Finally, Airflow triggers dbt to run schema tests (Data Quality) and build the final tables in the Gold layer, preserving historical changes.

🧠 Engineering Decisions & Key Optimizations

Hybrid FinOps Strategy: Delegating the heavy compute loads of Spark and Airflow orchestration to the local Ubuntu server avoided the high costs associated with managed services like AWS MWAA or AWS EMR.

Optimized Database Inserts (Batching): PySpark JDBC was configured with the batchsize=10000 parameter, preventing network bottlenecks and connection rejections in PostgreSQL when loading the Staging layer.

Historical Data Preservation (SCD Type 2): Instead of performing daily truncations and overwrites (DROP/INSERT), the Gold layer utilizes incremental dbt models. By using unique composite keys (id + updated_at), the database records a longitudinal history of fluctuations rather than just retaining the latest daily snapshot, enabling time-series analytics.
