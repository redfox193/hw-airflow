import os
from typing import Any
from airflow.models import BaseOperator
from airflow.utils.context import Context


class ReportGeneratorOperator(BaseOperator):

    def __init__(self, log_dir_path, log_level: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log_dir_path = log_dir_path
        self.log_level = log_level.upper()

    def execute(self, context: Context) -> Any:
        self.log.info(f'Reading log directory: {self.log_dir_path}')

        xcom_data = {}

        try:
            for log_filename in os.listdir(self.log_dir_path):
                if log_filename.endswith('.log'):
                    log_file_path = os.path.join(self.log_dir_path, log_filename)

                    self.log.info(f'Reading log file: {log_file_path}')

                    count = 0

                    try:
                        with open(log_file_path, 'r') as log_file:
                            for line in log_file:
                                if self.log_level in line:
                                    count += 1

                        xcom_data[log_filename] = {
                            f'{self.log_level.lower()}_count': count
                        }

                    except Exception as e:
                        self.log.error(f'Failed to generate report for {log_filename}: {str(e)}')

        except Exception as e:
            self.log.error(f'Failed to generate report for {self.log_dir_path}: {str(e)}')
            raise

        context['ti'].xcom_push(key='log_summary', value=xcom_data)
        self.log.info(f'Report generation complete')
