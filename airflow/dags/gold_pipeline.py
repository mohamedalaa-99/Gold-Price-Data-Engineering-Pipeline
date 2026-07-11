from doctest import master

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Sayed",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="gold_price_pipeline",
    default_args=default_args,
    description="Gold Price Data Pipeline",
    start_date=datetime(2026, 6, 27),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["gold", "kafka", "spark", "dbt"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    run_producer = BashOperator(
        task_id="run_kafka_producer",
        bash_command="""
        python /opt/airflow/kafka/Producer.py
        """,
        execution_timeout=timedelta(minutes=2)
    )

    run_consumer = BashOperator(
        task_id="run_kafka_consumer",
        bash_command="""
        python /opt/airflow/kafka/Consumer.py
        """,
        execution_timeout=timedelta(minutes=5)
    )

    run_spark = BashOperator(
    task_id="run_spark_etl",
    bash_command="""
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/airflow/pyspark_processing/sss.py
    """,
    execution_timeout=timedelta(minutes=10)
)

    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
        cd /opt/airflow/gold_price_dbt && \
        dbt run --profiles-dir /opt/airflow
        """,
        execution_timeout=timedelta(minutes=10)
    )

    finish = EmptyOperator(
        task_id="finish"
    )

    start >> run_producer >> run_consumer >> run_spark >> run_dbt >> finish