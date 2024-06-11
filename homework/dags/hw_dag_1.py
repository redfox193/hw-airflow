from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'sergei',
    'start_date': days_ago(1),
}

with DAG(dag_id='hw_dag_1', schedule_interval='@daily', default_args=default_args) as dag:
    t1 = DummyOperator(task_id='start', dag=dag)
    t2 = DummyOperator(task_id='op_1', dag=dag)
    t3 = DummyOperator(task_id='op_2', dag=dag)
    t4 = DummyOperator(task_id='some-other-task', dag=dag)
    t5 = DummyOperator(task_id='op_3', dag=dag)
    t6 = DummyOperator(task_id='op_4', dag=dag)
    t7 = DummyOperator(task_id='end', dag=dag)

    t1 >> [t2, t3] >> t4 >> [t5, t6] >> t7
