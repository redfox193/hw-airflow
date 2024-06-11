from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from .hook import HumanHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class FetchAndInsertHumanInfo(BaseOperator):
    def __init__(self, batch_size: int, http_conn_id: str, postgres_conn_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch_size = batch_size
        self.http_conn_id = http_conn_id
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context: Context) -> Any:
        http_hook = HumanHook(http_conn_id=self.http_conn_id)
        humans = http_hook.get_humans(self.batch_size)

        if len(humans) == 0:
            return

        values = []
        for human in humans:
            values.append(f"('{human['first_name']}', '{human['last_name']}', '{human['gender']}', '{human['email']}')")
        query = 'INSERT INTO humans (first_name, last_name, gender, email) VALUES' + ','.join(values) + ';'

        try:
            postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Error while inserting humans: {str(e)}")
