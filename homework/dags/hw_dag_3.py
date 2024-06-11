from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args = {
    'owner': 'sergei',
    'retries': 5,
    'retry_delay': timedelta(seconds=5),
}


def statistics(ti):
    http_dice_results = []
    system_dice_results = []

    for i in range(3):
        result = ti.xcom_pull(task_ids=f'http_get_dice_roll_{i+1}')
        if result is not None:
            http_dice_results.append(int(result))

    for i in range(3):
        result = ti.xcom_pull(task_ids=f'system_get_dice_roll_{i+1}')
        if result is not None:
            system_dice_results.append(int(result))

    all_dice_results = http_dice_results + system_dice_results
    print(f'Http dice roll: {http_dice_results}; system dice roll {system_dice_results}')
    print(f'Mean dice roll: {sum(all_dice_results) / len(all_dice_results)}')


with DAG(
    'dice_rolls',
    default_args=default_args,
    schedule_interval=timedelta(seconds=10),
    start_date=days_ago(1),
    catchup=False,
    tags=['hw_dag_3']
) as dag:
    http_dice_rolls = [
        HttpOperator(
            task_id=f'http_get_dice_roll_{i+1}',
            method='GET',
            response_filter=lambda response: str(response.json()['dice'][0]),
            http_conn_id='http_roll_dice',
            dag=dag,
        ) for i in range(3)
    ]

    system_dice_rolls = [
        BashOperator(
            task_id=f'system_get_dice_roll_{i+1}',
            bash_command='echo $(( ( RANDOM % 6 ) + 1 ))',
            dag=dag,
        ) for i in range(3)
    ]

    python_check = PythonOperator(
        task_id='print_statistics',
        python_callable=statistics,
        provide_context=True,
        dag=dag,
    )

    http_dice_rolls >> python_check
    system_dice_rolls >> python_check
