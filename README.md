# Gold-Price-Data-Engineering-Pipeline
An end-to-end Data Engineering pipeline that collects real-time and historical gold price data, processes it using Apache Spark, orchestrates workflows with Apache Airflow, transforms data with dbt, and builds an analytical Data Warehouse on Azure Databricks.

Project Overview
This project demonstrates a complete modern Data Engineering architecture.

The pipeline continuously ingests gold prices from an external API using Apache Kafka, stores raw data inside Azure Data Lake Storage Gen2, transforms it into a Silver layer using Apache Spark, then loads dimensional models into Azure Databricks using dbt.

Finally, the processed data is ready for visualization using Power BI.

Architecture
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
Technologies Used
Python
Apache Kafka
Apache Spark
Apache Airflow
dbt
Azure Data Lake Storage Gen2
Azure Databricks
Docker
Docker Compose
SQL
Power BI
Project Structure
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
Pipeline Workflow
Kafka Producer
Fetches live gold prices from Gold API.
Publishes JSON messages to Kafka Topic.
Kafka Consumer
Consumes Kafka messages.
Writes each record into Azure Data Lake Raw Layer.
Apache Spark ELT
Reads Streaming Data.
Reads Historical Batch Data..
Adds metadata.
Merges historical and streaming datasets.
Cleans and validates data.
Removes duplicates
Writes Delta Tables into the Silver Layer.
dbt
Transforms Silver Layer into an analytical Star Schema.

Models include:

stg_gold_price
dim_currency
dim_date
dim_time
fact_gold_price
Airflow
Orchestrates the complete pipeline every hour.

Workflow:

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
Azure Services
Azure Data Lake Storage Gen2
Azure Databricks SQL Warehouse
Data Warehouse Schema
Fact Table

fact_gold_price
Dimension Tables

dim_currency
dim_date
dim_time
Dashboard
The final dashboard includes:

Current Gold Price
Historical Trend
Daily Price Movement
Monthly Average
Quarterly Analysis
Currency Analysis
KPI Cards
Built using:

Power BI
Docker Services
Airflow Scheduler
Airflow Webserver
Airflow Init
PostgreSQL
Redis
Kafka
ZooKeeper
Kafdrop
Spark Master
Spark Worker
Run the Project
Clone the repository

git clone https://github.com/SayedR12/DEPI-Data-Engineering-Graduation-Project.git
Move into project

cd DEPI-Data-Engineering-Graduation-Project
Start Docker

docker compose up -d
Author
Mohamed Alaa
Data Analytics Engineer
GitHub:
