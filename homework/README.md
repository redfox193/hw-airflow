Перед тем как взаимодействовать с дагами, в UI Airflow надо настроить следующие connections:
```yaml
# connection for hw_dag_3
conn_id: http_get_human
conn_type: HTTP
host: https://qrandom.io/api/random/dice
```
```yaml
# connection for hw_dag_5
conn_id: http_get_human
conn_type: HTTP
host: https://randomuser.me/api/
```
```yaml
# connection for hw_dag_5
conn_id: postgres_homework
conn_type: Postgres
host: pgdb
database: postgres
login: tink
password: tink
port: 5432
```
Чтобы посмотреть, как даг заполняет бд, можно подключится к ней по ```localhost:5432```.