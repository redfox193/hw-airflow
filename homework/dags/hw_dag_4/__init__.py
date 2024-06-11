import os
from airflow import DAG
from .report_operator import ReportGeneratorOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import random
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

dag_folder = os.path.dirname(os.path.abspath(__file__))

log_dir_path = os.path.join(dag_folder, 'logs')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

report_ti_prefix = 'generate_report_'
reports_levels = ['error', 'info', 'warning']


def gather_reports(ti):
    summary = {}
    raw_summary = ti.xcom_pull(key='log_summary', task_ids=[report_ti_prefix + level for level in reports_levels])
    for rs in raw_summary:
        for file_name, log_report in rs.items():
            if file_name not in summary:
                summary[file_name] = log_report
            else:
                summary[file_name].update(log_report)
    print("XCom Data Received:")
    print(summary)


with DAG(
    'logs_parser',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['hw_dag_4']
) as dag:

    reports_generators = [
        ReportGeneratorOperator(
            task_id=report_ti_prefix + level,
            log_dir_path=log_dir_path,
            log_level=level,
            dag=dag
        )
        for level in reports_levels
    ]

    gather_reports = PythonOperator(
        task_id='gather_reports',
        provide_context=True,
        python_callable=gather_reports,
        dag=dag
    )

    reports_generators >> gather_reports
