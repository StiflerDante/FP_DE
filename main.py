import subprocess

import clickhouse_connect

# import clickhouse_connect
import pandas as pd
import pyspark
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
}

dag = DAG(
    "main",
    default_args=default_args,
    description="A simple DAG to interact with ClickHouse and PostgreSQL without libraries",
    schedule_interval=None,
)


def query_clickhouse(**kwargs):
    response = requests.get("http://clickhouse_user:8123/?query=SELECT%20version()")
    if response.status_code == 200:
        print(f"ClickHouse version: {response.text}")
    else:
        print(f"Failed to connect to ClickHouse, status code: {response.status_code}")


def query_postgres(**kwargs):
    command = [
        "psql",
        "-h",
        "postgres_user",
        "-U",
        "user",
        "-d",
        "test",
        "-c",
        "SELECT version();",
    ]
    env = {"PGPASSWORD": "password"}
    result = subprocess.run(command, env=env, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"PostgreSQL version: {result.stdout}")
    else:
        print(f"Failed to connect to PostgreSQL, error: {result.stderr}")


def panda():  # Лишняя функция для проверки библиотек
    data = {"Name": ["Tom", "nick", "krish", "jack"], "Age": [20, 21, 19, 18]}

    print(sys.path)

    df = pd.DataFrame(data)


def sp():
    spark = pyspark.SparkSession().builder.config("hello").getOrCreate()

    spark.stop()


def click():
    # Connect to ClickHouse using clickhouse_connect
    client = clickhouse_connect.get_client(
        host="clickhouse_user",  # Hostname of ClickHouse container
        port=8123,  # Default port for HTTP connection
    )

    # SQL query to create the table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS default.new_table (
        key UInt32,
        value String,
        metric Float64
    ) ENGINE = MergeTree 
    ORDER BY key
    """

    # Execute the query to create the table
    client.command(create_table_query)
    print("Table 'new_table' created successfully.")


click_query = PythonOperator(task_id="click", python_callable=click, dag=dag)

pandas_query = PythonOperator(
    task_id="panda", python_callable=panda, dag=dag
)  # Лишний оператор для проверки библиотек

task_query_clickhouse = PythonOperator(
    task_id="query_clickhouse",
    python_callable=query_clickhouse,
    dag=dag,
)

task_query_postgres = PythonOperator(
    task_id="query_postgres",
    python_callable=query_postgres,
    dag=dag,
)

click_query >> pandas_query >> task_query_clickhouse >> task_query_postgres
