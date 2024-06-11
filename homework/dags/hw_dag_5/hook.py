import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class HumanHook(BaseHook):

    def __init__(self, http_conn_id: str):
        super().__init__()
        self.conn_id = http_conn_id

    def get_humans(self, kol: int) -> list[dict]:
        url = self._get_api_url()

        data = []
        try:
            for _ in range(kol):
                response = requests.get(url)
                response.raise_for_status()

                user_data = response.json()['results'][0]

                data.append({'first_name': user_data['name']['first'],
                             'last_name': user_data['name']['last'],
                             'gender': user_data['gender'][0].upper(),
                             'email': user_data['email']
                             })
        except Exception as e:
            print(f"Error while getting data via API: {str(e)}")
            raise

        return data

    def _get_api_url(self):
        conn = self.get_connection(self.conn_id)
        if not conn.host:
            raise AirflowException('Missing API url in connection settings')
        return conn.host
