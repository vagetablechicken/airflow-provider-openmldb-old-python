import functools
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_provider_openmldb.operators.openmldb import OpenMLDBOperator

default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2021, 7, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

consumer_logger = logging.getLogger("airflow")

with DAG(
        "openmldb-example",
        default_args=default_args,
        description="Examples of Kafka Operators",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["example"],
) as dag:
    t1 = OpenMLDBOperator(
        task_id="create db",
        sql="CREATE DATABASE IF NOT EXISTS test_db"
    )
    t1.doc_md = ''

    t2 = OpenMLDBOperator(
        task_id="consume_from_topic",
        sql="CREATE TABLE IF NOT EXISTS test_airflow"
    )

    t2.doc_md = ''

    t3 = OpenMLDBOperator(
        task_id="produce_to_topic_2",
        sql=""
    )

    t3.doc_md = 'Does the same thing as the t1 task, but passes the callable directly instead of using the string ' \
                'notation. '

    t1 >> t2 >> t3
