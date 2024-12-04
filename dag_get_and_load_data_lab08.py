from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.models import Variable
import pendulum
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import os
import zipfile
from datetime import timedelta
import logging
import glob
from clickhouse_driver import Client

CLICKHOUSE_HOST = Variable.get("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = Variable.get("CLICKHOUSE_PORT")
CLICKHOUSE_DB = Variable.get("CLICKHOUSE_DB")
CLICKHOUSE_USER = Variable.get("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = Variable.get("CLICKHOUSE_PASSWORD")

default_args = {
    'owner': 'me_andrew',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 11, 15, tz="UTC"),
    catchup=True,
    default_args=default_args,
    tags=['pipeline_lab08'],
    max_active_runs=4
)
def download_and_process_data():
    bucket_name = 'npl-de15-lab8-data'
    endpoint_url = 'https://storage.yandexcloud.net'
    download_path = '/data/'
    os.makedirs(download_path, exist_ok=True)

    @task
    def clean_up_before_download():
        context = get_current_context()
        execution_date = context['logical_date']

        date_str = execution_date.strftime('%Y-%m-%d')
        date_path = os.path.join(download_path, date_str)

        import shutil
        try:
            if os.path.exists(date_path):
                shutil.rmtree(date_path)
                logging.info(f"Директория {date_path} очищена перед загрузкой новых данных.")
            os.makedirs(date_path, exist_ok=True)
        except Exception as e:
            logging.error(f"Ошибка при очистке директории {date_path}: {e}")
            raise

    @task
    def list_files():
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        context = get_current_context()
        execution_date = context['logical_date']
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            config=Config(signature_version=UNSIGNED)
        )

        year = execution_date.strftime('%Y')
        month = execution_date.strftime('%m')
        day = execution_date.strftime('%d')

        prefix = f'year={year}/month={month}/day={day}/'
        logging.info(f"Ищем файлы с префиксом: {prefix}")

        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        files = []
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.jsonl.zip'):
                    files.append({'key': key})
        logging.info(f"Найдено {len(files)} файлов для загрузки за {prefix}.")
        return files

    @task
    def download_and_unzip_files(files):
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            config=Config(signature_version=UNSIGNED)
        )

        context = get_current_context()
        execution_date = context['logical_date']

        date_str = execution_date.strftime('%Y-%m-%d')
        date_path = os.path.join(download_path, date_str)
        os.makedirs(date_path, exist_ok=True)

        for file_info in files:
            key = file_info['key']
            local_path = os.path.join(date_path, os.path.basename(key))

            try:
                s3_client.download_file(bucket_name, key, local_path)
                logging.info(f"Файл {key} скачан в {local_path}")

                if local_path.endswith('.zip'):
                    with zipfile.ZipFile(local_path, 'r') as zip_ref:
                        for file_name in zip_ref.namelist():
                            unique_prefix = key.replace('/', '_').replace('=', '_')
                            extracted_file_name = f"{unique_prefix}_{file_name}"
                            extracted_file_path = os.path.join(date_path, extracted_file_name)

                            with open(extracted_file_path, 'wb') as extracted_file:
                                extracted_file.write(zip_ref.read(file_name))
                            logging.info(f"Извлечен файл: {extracted_file_path}")

                    os.remove(local_path)
                    logging.info(f"Файл {local_path} удален после распаковки")
            except Exception as e:
                logging.error(f"Ошибка при обработке файла {key}: {e}")
                raise

    @task
    def delete_data_from_clickhouse():
        context = get_current_context()
        execution_date = context['logical_date']

        date_str = execution_date.strftime('%Y-%m-%d')

        client_clickhouse = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )

        tables = ['browser_events', 'mv_union_events']
        for table in tables:
            try:
                event_time_field = 'event_time' if  'mv_' in table else 'event_timestamp'
                query = f"ALTER TABLE {table} DELETE WHERE toDate({event_time_field}) = '{date_str}'"
                client_clickhouse.execute(query)
                logging.info(f"Данные из таблицы {table} за {date_str} удалены.")
            except Exception as e:
                logging.error(f"Ошибка при удалении данных из таблицы {table}: {e}")
                raise
        try:
            query_device = f"""
            ALTER TABLE device_events DELETE
            WHERE click_id NOT IN (
            SELECT click_id FROM browser_events);
            """
            client_clickhouse.execute(query_device)

            query_geo = f"""
            ALTER TABLE geo_events DELETE
            WHERE  click_id NOT IN (
            SELECT click_id FROM browser_events);
            """
            client_clickhouse.execute(query_geo)

            query_location = f"""
            ALTER TABLE location_events DELETE
            WHERE  event_id NOT IN (
            SELECT event_id FROM browser_events);
            """
            client_clickhouse.execute(query_location)

        except Exception as e:
            logging.error(f"Ошибка при удалении записей: {e}")
            raise

        client_clickhouse.disconnect()

    @task
    def load_data_to_clickhouse():
        import json
        from clickhouse_driver import Client

        context = get_current_context()
        execution_date = context['logical_date']

        date_str = execution_date.strftime('%Y-%m-%d')
        date_path = os.path.join(download_path, date_str)

        client_clickhouse = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )

        jsonl_files = glob.glob(os.path.join(date_path, '*.jsonl'))

        for jsonl_file in jsonl_files:
            if 'browser_events' in jsonl_file:
                table_name = 'browser_events'
            elif 'device_events' in jsonl_file:
                table_name = 'device_events'
            elif 'geo_events' in jsonl_file:
                table_name = 'geo_events'
            elif 'location_events' in jsonl_file:
                table_name = 'location_events'
            else:
                logging.warning(f"Неизвестный тип файла: {jsonl_file}")
                continue

            try:
                with open(jsonl_file, 'r') as f:
                    data = f.read()

                if data:
                    client_clickhouse.execute(
                        f"INSERT INTO {table_name} FORMAT JSONEachRow {data}"
                    )
                    logging.info(f"Данные из файла {jsonl_file} загружены в таблицу {table_name}.")
                else:
                    logging.warning(f"Файл {jsonl_file} пуст.")
            except Exception as e:
                logging.error(f"Ошибка при загрузке данных из файла {jsonl_file} в таблицу {table_name}: {e}")
                raise

        client_clickhouse.disconnect()

    @task
    def clean_up():
        context = get_current_context()
        execution_date = context['logical_date']

        date_str = execution_date.strftime('%Y-%m-%d')
        date_path = os.path.join(download_path, date_str)

        import shutil
        try:
            shutil.rmtree(date_path)
            logging.info(f"Директория {date_path} удалена.")
        except Exception as e:
            logging.error(f"Ошибка очистки {date_path}: {e}")
            raise

    files_to_download = list_files()

    with TaskGroup("download_and_unzip", tooltip="Скачивание и распаковка файлов") as download_and_unzip:
        download_and_unzip_files(files=files_to_download)

    (
        clean_up_before_download()
        >> download_and_unzip
        >> delete_data_from_clickhouse()
        >> load_data_to_clickhouse()
        >> clean_up()
    )

download_and_process_data = download_and_process_data()
