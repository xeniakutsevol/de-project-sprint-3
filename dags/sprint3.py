import json
import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


task_logger = logging.getLogger("airflow.task")

http_conn_id = HttpHook.get_connection("http_conn_id")
api_key = http_conn_id.extra_dejson.get("api_key")
base_url = http_conn_id.host

postgres_conn_id = "postgresql_de"

nickname = Variable.get("nickname")
cohort = Variable.get("cohort")

headers = {
    "X-Nickname": nickname,
    "X-Cohort": cohort,
    "X-Project": "True",
    "X-API-KEY": api_key,
    "Content-Type": "application/x-www-form-urlencoded",
}


def generate_report(ti):
    task_logger.info("Making request generate_report")

    response = requests.post(f"{base_url}/generate_report", headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)["task_id"]
    ti.xcom_push(key="task_id", value=task_id)
    task_logger.info(f"Response is {response.content}")


def get_report(ti):
    task_logger.info("Making request get_report")
    task_id = ti.xcom_pull(key="task_id")

    report_id = None

    for i in range(20):
        response = requests.get(
            f"{base_url}/get_report?task_id={task_id}", headers=headers
        )
        response.raise_for_status()
        task_logger.info(f"Response is {response.content}")
        status = json.loads(response.content)["status"]
        if status == "SUCCESS":
            report_id = json.loads(response.content)["data"]["report_id"]
            break
        else:
            time.sleep(10)

    if not report_id:
        task_logger.error("TimeoutError getting report_id.")
        raise TimeoutError("TimeoutError getting report_id.")

    ti.xcom_push(key="report_id", value=report_id)
    task_logger.info(f"Report_id={report_id}")


def get_increment(date, ti):
    task_logger.info("Making request get_increment")
    report_id = ti.xcom_pull(key="report_id")
    response = requests.get(
        f"{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00",
        headers=headers,
    )
    response.raise_for_status()
    task_logger.info(f"Response is {response.content}")

    increment_id = json.loads(response.content)["data"]["increment_id"]
    if not increment_id:
        task_logger.error(f"Increment is empty. Most probably due to error in API call.")
        raise ValueError(f"Increment is empty. Most probably due to error in API call.")

    ti.xcom_push(key="increment_id", value=increment_id)
    task_logger.info(f"increment_id={increment_id}")


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key="increment_id")
    s3_filename = f"https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}"
    task_logger.info(s3_filename)
    local_filename = date.replace("-", "") + "_" + filename
    task_logger.info(local_filename)
    response = requests.get(s3_filename)
    response.raise_for_status()
    open(f"{local_filename}", "wb").write(response.content)
    task_logger.info(response.content)

    # В csv-файлах два лишних столбца - индекс и id, оставлю такое преобразование.
    df = pd.read_csv(local_filename)
    df = df.drop('id', axis=1) 
    df = df.drop_duplicates(subset=["uniq_id"])

    if "status" not in df.columns:
        df["status"] = "shipped"
    
    df.to_csv(local_filename, index=False)

    postgres_hook = PostgresHook(postgres_conn_id)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"""
    drop table if exists staging.user_order_log_tmp;

    create table staging.user_order_log_tmp as
    select * from staging.user_order_log with no data;

    copy staging.user_order_log_tmp from '/opt/airflow/{local_filename}' delimiter ',' csv header;

    insert into staging.user_order_log
    (uniq_id, date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount, status)
    select * from staging.user_order_log_tmp
    on conflict(uniq_id) do update set
    (date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount, status)
    =
    (excluded.date_time, excluded.city_id, excluded.city_name, excluded.customer_id, excluded.first_name, excluded.last_name,
    excluded.item_id, excluded.item_name, excluded.quantity, excluded.payment_amount, excluded.status);
    """)
    conn.commit()
    task_logger.info(f"Data uploaded successfully.")


args = {
    "owner": "student",
    "email": ["student@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
}

business_dt = "{{ ds }}"

with DAG(
    "sales_mart",
    default_args=args,
    description="Provide default dag for sprint3",
    catchup=True,
    start_date=datetime.today() - timedelta(days=7),
    end_date=datetime.today() - timedelta(days=1),
) as dag:
    generate_report = PythonOperator(
        task_id="generate_report", python_callable=generate_report
    )

    get_report = PythonOperator(task_id="get_report", python_callable=get_report)

    get_increment = PythonOperator(
        task_id="get_increment",
        python_callable=get_increment,
        op_kwargs={"date": business_dt},
    )

    upload_user_order_inc = PythonOperator(
        task_id="upload_user_order_inc",
        python_callable=upload_data_to_staging,
        op_kwargs={
            "date": business_dt,
            "filename": "user_order_log_inc.csv",
            "pg_table": "user_order_log",
            "pg_schema": "staging",
        },
    )

    update_d_item_table = PostgresOperator(
        task_id="update_d_item",
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql",
    )

    update_d_customer_table = PostgresOperator(
        task_id="update_d_customer",
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql",
    )

    update_d_city_table = PostgresOperator(
        task_id="update_d_city",
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql",
    )

    update_f_sales = PostgresOperator(
        task_id="update_f_sales",
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}},
    )

    update_f_customer_retention = PostgresOperator(
        task_id="update_f_customer_retention",
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql",
        parameters={"date": {business_dt}},
    )

    (
        generate_report
        >> get_report
        >> get_increment
        >> upload_user_order_inc
        >> [update_d_item_table, update_d_city_table, update_d_customer_table]
        >> update_f_sales
        >> update_f_customer_retention
    )
