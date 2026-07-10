#  Gold Price Data Engineering Pipeline

An end-to-end **Data Engineering Pipeline** that collects both **real-time** and **historical** gold price data, processes it using **Apache Spark**, orchestrates workflows with **Apache Airflow**, transforms data with **dbt**, and builds an analytical **Data Warehouse** on **Azure Databricks**.

The final curated data is visualized through an interactive **Power BI Dashboard**.

---

#  Project Overview

This project demonstrates a complete modern **Data Engineering Architecture** following the **Medallion Architecture (Raw → Silver → Warehouse)**.

The pipeline continuously:

-  Fetches live gold prices from an external API.
-  Streams data using Apache Kafka.
-  Stores raw data in Azure Data Lake Storage Gen2.
-  Processes and cleans data using Apache Spark.
-  Builds dimensional models using dbt.
-  Loads the analytical warehouse into Azure Databricks.
-  Visualizes insights using Power BI.

---

#  Architecture

```text
                Gold Price API
                      │
                      ▼
              Kafka Producer
                      │
                      ▼
                Kafka Topic
                      │
                      ▼
              Kafka Consumer
                      │
                      ▼
      Azure Data Lake (Raw Layer)
                      │
                      ▼
             Apache Spark ETL
                      │
                      ▼
     Azure Data Lake (Silver Layer)
                      │
                      ▼
                 dbt Models
                      │
                      ▼
      Azure Databricks Warehouse
                      │
                      ▼
            Power BI Dashboard
```

---

#  Technologies Used

| Category | Technologies |
|----------|--------------|
| Programming | Python |
| Streaming | Apache Kafka |
| Processing | Apache Spark (PySpark) |
| Orchestration | Apache Airflow |
| Transformation | dbt |
| Storage | Azure Data Lake Storage Gen2 |
| Data Warehouse | Azure Databricks |
| Containerization | Docker, Docker Compose |
| Database | SQL |
| Visualization | Power BI |

---

#  Project Structure

```text
DEPI_PROJECT
│
├── airflow/
│   ├── dags/
│   ├── config/
│   └── profiles.yml
│
├── kafka/
│   ├── Producer.py
│   ├── Consumer.py
│   └── gold_api.py
│
├── spark/
│   └── silver_layer.py
│
├── gold_price_dbt/
│   ├── models/
│   ├── macros/
│   └── dbt_project.yml
│
├── data_lake/
│
├── pyspark_processing/
│
├── docker-compose.yml
│
└── Dockerfile.airflow
```

---

#  Pipeline Workflow

##  Kafka Producer

- Fetches live gold prices from the Gold Price API.
- Publishes JSON messages to a Kafka Topic.

---

##  Kafka Consumer

- Consumes Kafka messages.
- Writes each record into the Azure Data Lake Raw Layer.

---

##  Apache Spark ETL

The Spark pipeline performs the following tasks:

- Reads streaming data.
- Reads historical batch data.
- Adds metadata.
- Merges historical and streaming datasets.
- Cleans and validates data.
- Removes duplicates.
- Writes Delta Tables into the Silver Layer.

---

##  dbt

Transforms the Silver Layer into an analytical **Star Schema**.

### Models

- `stg_gold_price`
- `dim_currency`
- `dim_date`
- `dim_time`
- `fact_gold_price`

---

##  Apache Airflow

Airflow orchestrates the complete pipeline every hour.

```text
Start
   │
   ▼
Kafka Producer
   │
   ▼
Kafka Consumer
   │
   ▼
Spark ETL
   │
   ▼
dbt Models
   │
   ▼
Finish
```

---

#  Azure Services

- Azure Data Lake Storage Gen2
- Azure Databricks SQL Warehouse

---

#  Data Warehouse Schema

## Fact Table

- `fact_gold_price`

## Dimension Tables

- `dim_currency`
- `dim_date`
- `dim_time`

---

#  Power BI Dashboard

The dashboard provides:

-  Current Gold Price
-  Historical Price Trend
-  Daily Price Movement
-  Monthly Average
-  Quarterly Analysis
-  Currency Analysis
-  KPI Cards

---

#  Docker Services

The project runs the following services:

- Apache Airflow Scheduler
- Apache Airflow Webserver
- Apache Airflow Init
- PostgreSQL
- Redis
- Apache Kafka
- ZooKeeper
- Kafdrop
- Spark Master
- Spark Worker

---

#  Run the Project

## 1️⃣ Clone the Repository

```bash
git clone https://github.com/SayedR12/DEPI-Data-Engineering-Graduation-Project.git
```

## 2️⃣ Navigate to the Project

```bash
cd DEPI-Data-Engineering-Graduation-Project
```

## 3️⃣ Start Docker Services

```bash
docker compose up -d
```

---

# Author

**Mohamed Alaa**

**Data Analytics Engineer**

GitHub: github.com/mohamedalaa-99

---

# ⭐ If you found this project useful, don't forget to give it a Star!
