# ♟️ Chess.com Analytics Pipeline

## Overview

This project builds an end-to-end data pipeline to analyze my personal performance on Chess.com (puzzles at first, matches at a later stage)

Semi-structured data is ingested, stored in a cloud data lake, modeled into a warehouse and prepared for analytics.

<img width="1512" height="930" alt="image" src="https://github.com/user-attachments/assets/c94db257-0568-40d7-a80a-f9ba9b95e26c" />

---

## Ingestion Layer (Python)

* Python scripts fetch data from chess.com web endpoint
* Data is normalized into valid JSON format, stored in Google Cloud Storage
* Incremental raw table is managed in BigQuery, with full payload and ingestion timestamps
* Acts as the single source of truth

### Key Features

* Environment-based configuration (local + cloud)
* Cloud Run deployment (serverless execution)
* Allows for periodic scheduling
* Raw data preserved for traceability

## Transformation Layer (dbt)

### Layered Modeling Approach

#### 1. Staging (`stg_`)

* Parse JSON fields
* Cast data types
* Standardize column names

#### 2. Intermediate (`int_`)

* Derive metrics (e.g., time in seconds)
* Handle timezone conversions
* Normalize multi-value fields (tags)

#### 3. Marts (`d_`, `f_`, `link_`)

Implements a star schema:

* **Fact Table**

  * `f_puzzle_attempts`: one row per puzzle attempt
  * Incremental model (partitioned + clustered)

* **Dimensions**

  * `d_puzzles`: puzzle attributes
  * `d_tags`: unique tag list

* **Bridge Table**

  * `link_puzzle_tag`: handles many-to-many relationship between puzzles and tags
 
#### 4. Analytics (`agg_`, `evol_`)

Sample queries for comparison in BI tools

### Key Features

* JSON handling and casting
* Use of custom dbt macros and tests
* ELT pipeline design (raw → staging → marts)
* Different materialization types (views, tables, incremental)
* Partitioning and clustering in BigQuery
* Dimensional modeling (star schema + bridge tables)

---

## Future Improvements

* Include data related to chess matches, not only puzzles
* BI dashboard for data visualization
