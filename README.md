# End-to-End E-commerce Analytics Pipeline on Databricks

This project demonstrates an end-to-end analytics pipeline built on **Databricks** using a **Lakehouse architecture**.  
It covers raw data ingestion, data modeling, business KPI development, and cohort-based customer retention analysis.

---

## Architecture Overview

The pipeline follows a **Bronze → Silver → Gold** layered design:

### 🥉 Bronze Layer – Raw Ingestion
- Ingested raw e-commerce CSV datasets into Databricks tables
- Preserved original schema and values for traceability

### 🥈 Silver Layer – Data Modeling & Transformation
- Built **clean fact tables**, **dimension tables**, and **enriched datasets**
- Standardized data types (timestamps, numeric fields)
- Applied business logic using **Spark SQL**
- Designed reusable, analysis-ready tables

### 🥇 Gold Layer – Business Analytics
- Developed business-facing KPI tables:
  - Daily GMV
  - Order volume
  - Average Order Value (AOV)
- Built **cohort-based customer retention analysis**
  - Cohorts defined by customers’ first purchase month
  - Retention tracked using `customer_unique_id` to ensure correct user-level analysis

---

## Key Features

- Spark SQL–based transformations on Databricks
- Fact and dimension data modeling
- Gold-layer KPI aggregation for reporting
- Cohort retention analysis to evaluate repeat purchasing behavior
- End-to-end pipeline design from raw data to analytical insights

---

## Notebooks

The project is organized into the following Databricks notebooks:

- `01_bronze_ingestion` – Raw data ingestion
- `02_silver_transformation` – Data cleaning and modeling
- `03_gold_kpi` – Business KPI aggregation
- `04_cohort_kpi` – Customer retention and cohort analysis

> **Note:** Notebooks are exported from Databricks Community Edition as DBC archives due to platform limitations.  
> SQL logic is also provided in readable `.sql` files for easy review.

---

## Technologies Used

- Databricks (Community Edition)
- Spark SQL
- SQL-based data modeling
- Lakehouse architecture (Bronze / Silver / Gold)

---

## Business Use Cases

- Executive-level revenue and order monitoring
- Customer purchasing behavior analysis
- Retention and cohort analysis for growth insights
- Reusable analytics tables for BI and reporting

---

## Notes

This project is intended for **portfolio and demonstration purposes**.  
Notebooks are provided for code review and architectural understanding rather than direct execution.

---
