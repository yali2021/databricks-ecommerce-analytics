# Data Transformation and Analytics with Databricks
E-commerce data transformation project using Databricks and Spark SQL with data cleaning, quality checks, and Silver/Gold layers.
---
## Project Overview

This project uses an e-commerce dataset to practice data transformation and analysis in Databricks.

The data is organized into Bronze, Silver, and Gold layers. Raw CSV files are loaded into the Bronze layer, cleaned and transformed in the Silver layer, and aggregated into business metrics in the Gold layer.

## Data Pipeline

### Bronze

Raw e-commerce CSV files are loaded into Databricks tables without changing the original values.

### Silver

The raw data is cleaned and transformed using Spark SQL.

Main steps include:
- Standardizing timestamps and numeric fields
- Creating fact and dimension tables
- Applying business rules and transformations
- Preparing cleaned tables for analysis

### Gold

Gold tables contain aggregated metrics used for analysis, including:
- Daily GMV
- Order volume
- Average Order Value (AOV)

---

## Key Features

- Data transformation using Spark SQL in Databricks
- Fact and dimension table creation
- Data quality checks and data cleaning
- Gold-layer tables for sales trends and average order value

---

## Notebooks

The project includes three Databricks notebooks:

- `01_bronze_ingestion` – Loads raw e-commerce data into Bronze tables
- `02_silver_transformation` – Cleans and transforms the data into fact and dimension tables
- `03_gold_kpi` – Creates Gold tables for sales trends and average order value


## Technologies

- Databricks Community Edition
- Spark SQL
- Bronze / Silver / Gold data layers

---

