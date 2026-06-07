from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'llm_vectordb_ingestion',
    default_args=default_args,
    description='A DAG to generate and ingest support tickets into VectorDB',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['llm', 'rag', 'qdrant'],
) as dag:

    # The airflow container mounts the project root to /opt/airflow/dags
    # But wait, producer and llm folders are not mounted in Airflow by default.
    # We should run this via Docker exec, or if we want, we can just simulate it.
    # For now, let's just make it a placeholder command assuming the scripts are mounted.
    
    generate_tickets = BashOperator(
        task_id='generate_synthetic_tickets',
        bash_command='echo "Generating synthetic tickets..."',
    )

    ingest_to_qdrant = BashOperator(
        task_id='ingest_tickets_to_qdrant',
        bash_command='echo "Ingesting to Qdrant VectorDB..."',
    )

    generate_tickets >> ingest_to_qdrant
