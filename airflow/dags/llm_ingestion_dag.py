from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

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
    description='Generate synthetic support tickets and ingest them into Qdrant VectorDB for RAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['llm', 'rag', 'qdrant'],
) as dag:

    generate_tickets = BashOperator(
        task_id='generate_synthetic_tickets',
        bash_command='cd /opt/airflow && python producer/ticket_generator.py',
    )

    ingest_to_qdrant = BashOperator(
        task_id='ingest_tickets_to_qdrant',
        bash_command='cd /opt/airflow && python llm/ingest_to_vectordb.py',
    )

    generate_tickets >> ingest_to_qdrant
