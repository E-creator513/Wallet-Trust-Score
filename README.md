# Ethereum ELT & Wallet Trust Scoring Pipeline

##  Project Overview

This project implements an end-to-end ELT pipeline for Ethereum blockchain data.  
The system ingests raw blockchain data, transforms it into structured analytical models using dbt, and computes wallet-level trust metrics using machine learning.

The architecture follows modern data engineering principles:

- Separation of ingestion, transformation, and analytics
- Layered modeling approach (STG → ODS → DWH → DM)
- Incremental processing for scalability
- Automated testing and documentation
- Orchestrated execution via Airflow


![image](https://github.com/E-creator513/EL-process8/blob/master/airf.png)
The objective is to demonstrate best practices in building production-style analytical pipelines.

---

## Architecture

### Data Flow

1. **Data Ingestion**
   - Ethereum blocks and transactions are fetched.
   - Raw data is stored in MongoDB.

2. **Data Loading**
   - Raw data is transferred from MongoDB to PostgreSQL.
   - PostgreSQL acts as the analytical warehouse.

3. **Data Transformation (dbt)**
   - Data is transformed into structured analytical models.
   - Incremental loading strategies are applied.
   - Tests and documentation are generated.

4. **Analytical Modeling**
   - Wallet-level metrics are calculated.
   - A Wallet Trust Score (WTS) is computed using Isolation Forest.

5. **Orchestration**
   - All steps are scheduled and executed via Airflow.

The pipeline follows an **ELT paradigm**:

```
Extract → Load → Transform
```

---

##  dbt Modeling Strategy

### Layered Architecture

Models are separated by functional layers:

- **STG (Staging)**
  - Data cleaning and normalization
  - Column renaming and type casting
  - Removal of inconsistencies

- **ODS (Operational Data Store)**
  - Structured but still granular data
  - Suitable for operational analytics

- **DWH (Data Warehouse)**
  - Aggregated, business-level models
  - Wallet metrics and block summaries

- **DM (Data Marts)**
  - Final analytical outputs (e.g., wallet trust scores)

This separation ensures clarity, maintainability, and scalability.

---

##  Incremental Loading Strategies

To support scalability and efficiency, two incremental strategies were implemented:

### 1  Timestamp-Based Incremental Loading

Models load only new records using a `fetched_at` timestamp:

- Filters new data
- Reduces compute cost
- Prevents full table rebuilds

### 2 Unique Key Merge Strategy

Incremental models use:

```sql
{{ config(
    materialized='incremental',
    unique_key='wallet',
    incremental_strategy='merge'
) }}
```

This ensures:

- Idempotent updates
- Correct handling of late-arriving data
- Stable warehouse state across reruns

Incremental loading is critical for blockchain-scale datasets.

---

## ✅ Testing in dbt

Testing ensures data reliability and trustworthiness.

### Built-in Tests

The following built-in tests are applied:

- `unique`
- `not_null`
- `relationships`

These tests validate:

- Primary key integrity
- Data completeness
- Referential consistency

### Custom Tests

Custom SQL tests validate business logic constraints, such as:

- Non-negative transaction values
- Logical consistency checks

Example:

```sql
select *
from {{ ref('stg_transactions') }}
where value < 0
```

If rows are returned, the test fails.

---

##  Data Observability with Elementary

Elementary is integrated to enhance monitoring and observability.

Its role includes:

- Detecting volume anomalies
- Monitoring freshness
- Identifying schema changes
- Tracking test failures over time

This adds a data quality monitoring layer on top of dbt transformations.

---

##  Documentation

All models and sources are documented using `schema.yml`.

Documentation includes:

- Source descriptions
- Model purposes
- Column-level explanations
- Test definitions

Documentation is generated via:

```bash
dbt docs generate
dbt docs serve
```

This ensures transparency and maintainability.

---

##  Orchestration with Airflow

The entire pipeline is scheduled and orchestrated via Airflow.

The DAG executes:

1. Fetch Ethereum data
2. Load into PostgreSQL
3. Run dbt models
4. Execute dbt tests
5. Compute wallet trust score

This enables:

- Automated scheduling
- Reproducibility
- Dependency management
- Production-style workflow execution

---

##  Wallet Trust Score (WTS)

Wallet Trust Score is computed using machine learning techniques.

Features include:

- Total transaction volume
- Transaction frequency
- Gas usage patterns
- Block participation metrics

An Isolation Forest model is applied to detect anomalous wallet behavior.

The resulting score:

- Quantifies behavioral normality
- Enables risk classification
- Demonstrates integration of data engineering and ML workflows

---

##  Key Design Principles

The project emphasizes:

- Modularity
- Reproducibility
- Scalability
- Testability
- Observability

dbt is used not only for SQL transformations, but as a transformation framework with testing, documentation, and incremental logic built in.

---

##  Conclusion

This project demonstrates how to construct a structured, production-style ELT pipeline for blockchain analytics.

It integrates:

- MongoDB (raw storage)
- PostgreSQL (warehouse)
- dbt (transformation layer)
- Elementary (monitoring)
- Airflow (orchestration)
- Machine Learning (analytics layer)

The result is a complete, automated, and scalable analytical workflow.