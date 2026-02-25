# AWS Glue Lakehouse ETL Pipeline
End-to-end Lakehouse ETL pipeline built on AWS using Glue (PySpark) and a layered Bronze, Silver and Gold architecture.

## 🏗 Architecture Diagram

![Architecture](docs/architecture.png)
## Project Overview

This project demonstrates the implementation of a complete data pipeline using:

- AWS Glue ETL Jobs built with PySpark
- Amazon S3 (Data Lake storage)
- PySpark
- AWS Data Catalog
- Athena
- Scheduled and Conditional Triggers

The pipeline simulates a real-world data engineering workflow with orchestration between layers.
---
## Architecture

Schedule Trigger (optional)
        ↓
Crawler Bronze
        ↓
Trigger Silver (Conditional)
        ↓
Job Silver (Data transformation)
        ↓
Trigger Gold (Conditional)
        ↓
Job Gold (Business aggregation)

---

## Layer Description

### Bronze Layer
- Raw data ingestion
- Schema discovery via Glue Crawler
- Stored in S3 as raw datasets

### Silver Layer
- Data cleaning and transformation
- Standardization and enrichment
- Processed with PySpark

### Gold Layer
- Aggregated and business-ready datasets
- Optimized for analytics and Athena queries
---
## Orchestration

- Glue Workflow coordinates all stages
- Conditional triggers ensure dependency order
- Scheduled trigger can be activated for automated execution
---
## Technologies Used
- AWS Glue
- Amazon S3
- PySpark
- AWS Data Catalog
- Amazon Athena
- Parquet format
---

## 🧠 Design Decisions

- Bronze/Silver/Gold architecture was chosen to separate raw ingestion, transformation, and business-level aggregation.
- Data in the Silver layer is partitioned by `year` and `month` to optimize query performance in Athena.
- AWS Glue Crawler is used for automatic schema discovery and metadata registration in the Data Catalog.
- Glue Workflows combined with EventBridge Scheduler enable orchestration and automation of the pipeline.
- Parquet format is used in processed layers for columnar storage efficiency.
  
## Purpose

This project was developed to simulate a production-ready data engineering pipeline using AWS-native services and modern Lakehouse architecture principles.
---
## Author

Manoel Alexandre Peres
