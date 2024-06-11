from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from .operator import FetchAndInsertHumanInfo


default_args = {
    'owner': 'sergei',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    'fake_human_data_collector',
    default_args=default_args,
    schedule_interval=timedelta(seconds=10),
    start_date=days_ago(1),
    catchup=False,
    tags=['hw_dag_5']
) as dag:
    create_table = PostgresOperator(
        task_id='create_table_human',
        sql='sql/create_table.sql',
        postgres_conn_id='postgres_homework',
        dag=dag
    )

    fetch_humans = FetchAndInsertHumanInfo(
        task_id='fetch_humans',
        batch_size=5,
        http_conn_id='http_get_human',
        postgres_conn_id='postgres_homework',
        dag=dag
    )

    create_table >> fetch_humans

