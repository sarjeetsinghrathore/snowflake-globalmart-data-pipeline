# Snowflake GlobalMart Data Pipeline Project

## Project Overview

This project demonstrates an end-to-end Data Engineering pipeline built using Snowflake and AWS S3 by implementing the Medallion Architecture (Bronze, Silver, and Gold layers).

The primary objective of this project is to ingest retail sales data from AWS S3 into Snowflake, perform data transformation, and create business-ready analytical tables for reporting and analysis.

---

## Tech Stack

* Snowflake
* SQL
* AWS S3
* Storage Integration
* External Stage
* Medallion Architecture
* Data Warehousing

---

## Architecture

```text
AWS S3
   │
   ▼
Storage Integration
   │
   ▼
External Stage
   │
   ▼
Bronze Layer
(Raw Data)
   │
   ▼
Silver Layer
(Cleansed Data)
   │
   ▼
Gold Layer
(Business Data Marts)
```

---

## Database Structure

### Database

GLOBAL_MART_DB

### Schemas

* STORAGE_INTEGRATION
* BRONZE
* SILVER
* GOLD

---

## Pipeline Workflow

### Bronze Layer

* Raw data ingestion from AWS S3.
* Stores source data without transformation.

### Silver Layer

* Data cleaning.
* Data standardization.
* Duplicate removal.
* Data quality validation.

### Gold Layer

* Business aggregations.
* KPI generation.
* Analytical reporting tables.

---

## Key Features

✔ AWS S3 Integration with Snowflake
✔ Storage Integration Implementation
✔ External Stage Creation
✔ Medallion Architecture
✔ Retail Analytics Data Mart
✔ Business KPI Reporting
✔ End-to-End ETL Pipeline

---

## Business Metrics Generated

* Total Revenue
* Total Sales
* Average Order Value
* Customer Analysis
* Product Performance
* Store Performance
* Category Analysis

---

## Project Outcome

Successfully designed and implemented a scalable retail data warehouse solution using Snowflake that supports business analytics and reporting requirements.

---

## Author

Sarjeet Singh Rathore 

Aspiring Data Engineer
