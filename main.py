from urllib.parse import urlencode

import clickhouse_connect
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
}

dag = DAG(
    "main",
    default_args=default_args,
    description="A DAG that will work with pyspark data and store the results in clickhouse tables.",
    schedule_interval=None,
)


# Функции
def download_file(
    **kwargs,
):  # 1. Функция для скачивания файла и сохранения его в xcom для последующей передачи
    base_url = "https://cloud-api.yandex.net/v1/disk/public/resources/download?"
    public_key = "https://disk.yandex.ru/d/5MjUOaf08kUQJA"  # public link

    final_url = base_url + urlencode(dict(public_key=public_key))

    response = requests.get(final_url)

    if response.status_code == 200:
        try:
            download_url = response.text.split('"href":"')[1].split('"')[0]

            if download_url:
                download_response = requests.get(download_url)

                local_filename = "/opt/airflow/dags/downloaded_file.csv"

                with open(local_filename, "wb") as f:
                    f.write(download_response.content)

                kwargs["ti"].xcom_push(key="file_path", value=local_filename)

                print(f"Файл сохранен - {local_filename}")
            else:
                print("Download URL is missing.")
        except Exception as e:
            print(f"Error processing the file: {e}")
    else:
        print(f"Error: {response.status_code}, {response.text}")


def create_table_in_clickhouse():  # Создаем таблицу в кликхаусе
    client = clickhouse_connect.get_client(host="clickhouse_user", port=8123)

    # Create the table with the appropriate schema
    create_table_query = """
    CREATE TABLE IF NOT EXISTS default.houses_main (
        house_id Int32,
        latitude Float64,
        longitude Float64,
        maintenance_year Nullable(Int32),
        square Nullable(Float64),
        population Nullable(Int32),
        region Nullable(String),
        locality_name Nullable(String),
        address String,
        full_address String,
        communal_service_id Float64,
        description String
    ) ENGINE = MergeTree
    ORDER BY house_id;
    """
    client.command(create_table_query)

    create_regions_query = """
    CREATE TABLE IF NOT EXISTS default.regions (
        region Nullable(String),
        locality_name Nullable(String),
        total_objects Nullable(Int32)    
    ) ENGINE = MergeTree
    ORDER BY total_objects DESC;
    """
    client.command(create_regions_query)

    create_buildings_query = """
    CREATE TABLE IF NOT EXISTS default.buildings_count(
        decade Int64,
        building_count int64
        
    ) ENGINE = MergeTree
    """
    client.command(create_buildings_query)

    create_min_area_query = """
    CREATE TABLE IF NO EXISTS default.min_area(
        region Nullable(String)
        min_area Float64
    ) ENGINE = MergeTree
    
    """
    client.command(create_min_area_query)

    create_max_area_query = """
    CREATE TABLE IF NO EXISTS default.min_area(
        region Nullable(String)
        max_area Float64
    ) ENGINE = MergeTree
    
    """

    client.command(create_max_area_query)

    print("Table 'houses' created successfully.")


def spark_test(**kwargs):  # test, удалить после проверки спарка
    spark = (
        SparkSession.builder.config("Spark.name.app", "test")
        .master("local[*]")
        .getOrCreate()
    )

    csv_path = kwargs["ti"].xcom_pull(task_ids="download_file_task", key="file_path")

    df = spark.read.csv(csv_path, header=False, inferSchema=True)

    output_path = "/path/to/output_directory/saved_file.csv"  # Adjust path as needed

    # Save the DataFrame as a CSV file
    df.write.csv(output_path, header=True, mode="overwrite")

    # Push the saved file path back to XCom
    kwargs["ti"].xcom_push(key="file_path_spark", value=output_path)


def upload_csv_to_clickhouse(**kwargs):
    # Connect to ClickHouse
    client = clickhouse_connect.get_client(
        host="clickhouse_user",  # Adjust to match your container's name or IP
        port=8123,
    )

    # Get the path of the downloaded CSV file
    csv_path = kwargs["ti"].xcom_pull(task_ids="download_file_task", key="file_path")

    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_path)

    # Preprocess data to match the ClickHouse schema
    df["maintenance_year"] = pd.to_numeric(df["maintenance_year"], errors="coerce")
    df["square"] = pd.to_numeric(df["square"], errors="coerce")
    df["population"] = pd.to_numeric(df["population"], errors="coerce")
    df = df.drop("Unnamed: 0", axis=1)

    # Insert the DataFrame into ClickHouse using insert_dataframe
    client.insert_df("default.houses_main", df)

    print("CSV data uploaded to ClickHouse successfully.")


download_file_task = PythonOperator(
    task_id="download_file_task",
    python_callable=download_file,
    dag=dag,
)


spark_task = PythonOperator(
    task_id="spark_task",
    python_callable=spark_test,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id="create_table_task", python_callable=create_table_in_clickhouse, dag=dag
)


csv_to_click_task = PythonOperator(
    task_id="upload_csv_task", python_callable=upload_csv_to_clickhouse, dag=dag
)

# Таски

download_file_task >> spark_task >> create_table_task >> csv_to_click_task
