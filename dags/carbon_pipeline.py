# REDACTED VERSION

# Core imports for Apache Airflow
from airflow import DAG
from datetime import datetime
import pendulum

from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email import EmailOperator

# Imports for directory and config file management
from io import BytesIO, StringIO
import logging
import configparser
import pathlib

# Imports to handle extraction stage
import requests
import json

# Imports for transformation stage
import plotly.express as px
import pandas as pd
import numpy as np

# Imports for AWS
import boto3


import pprint as pp


# Dag setup
default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1,
                "start_date": datetime(2023, 10, 18)}
dag = DAG('carbon_pipeline', default_args=default_args, schedule_interval='@daily')

date = str(pendulum.today().date()).replace('-', "")

# Data for testing email
EMAIL_RECIPIENTS = ["testemail@example.com"]
EMAIL_SUBJECT = "Daily Carbon Update"
EMAIL_HTML_BODY = "Hi! This is your daily carbon intensity update"

# Function to manage parallel tasks
def branch_function(**kwargs):
    condition = True
    if condition:
        return 'convert_to_df'
    else:
        return 'write_to_s3'

# Function to manage API connection and extraction stage
def api_connection():
    headers = {'Accept': 'application/json'}
    postcode = 'EC2Y'
    try:
        r = requests.get(f'https://api.carbonintensity.org.uk/regional/postcode/{postcode}',
                         params={}, headers=headers)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Unable to connect to API. Error: {e}")

# Function that would handle extraction if separated from api connection function
# def return_carbon_json(**kwargs):
#     ti = kwargs['ti']
#     response_data = ti.xcom_pull(task_ids='api_connection')  
#     print(response_data)
#     return response_data


# Function to write to AWS S3. Use your own connection and specify where required.
def write_to_s3():
    conn = boto3.resource("s3")
    conn.meta.client.upload_file(Filename="/tmp/" + FILENAME, Bucket=BUCKET_NAME, Key=KEY)

# Function to handle data transformation
def convert_to_df(**kwargs):
    ti = kwargs['ti']
    response_data = ti.xcom_pull(task_ids='api_connection')
    data = response_data['data']
    generation_mix = data[0]['data'][0]['generationmix'][:]
    ti.xcom_push(key='generation_mix', value=generation_mix)
    return generation_mix


# Function to handle email distribution
def email_fig(**kwargs):
    ti = kwargs['ti']
    generation_mix = ti.xcom_pull(key='generation_mix', task_ids='convert_to_df')
    carbon_df = pd.DataFrame(generation_mix)

    fig_html = carbon_df.to_html()

    # Email content
    email_task = EmailOperator(
        task_id="send_email",
        to=EMAIL_RECIPIENTS,
        subject=EMAIL_SUBJECT,
        html_content=EMAIL_HTML_BODY + fig_html,
        dag=dag,
    )

    return 'send_email'

# Extract config info. Use your own config files and read.
# parser = configparser.ConfigParser()
# script_path = pathlib.Path(__file__).parent.resolve()
# parser.read(f"{script_path}/config.conf")


# Tasks as will be defined and seen in airflow localhost

task_1 = PythonOperator(
    task_id="api_connection", python_callable=api_connection, dag=dag,
)

branch_operator_task = BranchPythonOperator(
    task_id="branch_operator", provide_context=True, python_callable=branch_function, dag=dag,
)

task_2 = PythonOperator(
    task_id="convert_to_df", python_callable=convert_to_df,provide_context=True, dag=dag,
)

task_3 = PythonOperator(
    task_id="write_to_s3", python_callable=write_to_s3, dag=dag,
)

task_4 = PythonOperator(
    task_id="email_fig",
    python_callable=email_fig,
    provide_context=True,
    dag=dag,
)

# Task order management structure

task_1 >> branch_operator_task >> [task_2, task_3]
task_2 >> task_4