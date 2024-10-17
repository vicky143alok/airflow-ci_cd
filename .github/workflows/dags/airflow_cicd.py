from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import boto3
import pandas as pd
import os

# AWS credentials from environment variables
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'upload_to_s3',
    default_args=default_args,
    description='Upload table to S3',
    schedule_interval='@daily',
)

# Function to generate table (example DataFrame)
def generate_table():
    data = {
        'id': [1, 2, 3],
        'name': ['John', 'Jane', 'Doe'],
        'email': ['john@example.com', 'jane@example.com', 'doe@example.com']
    }
    df = pd.DataFrame(data)
    file_path = '/tmp/table.csv'
    df.to_csv(file_path, index=False)
    return file_path

# Function to upload file to S3
def upload_to_s3(file_path):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    bucket_name = 'my-s3-bucket'
    s3.upload_file(file_path, bucket_name, 'table.csv')

# Tasks for DAG
generate_task = PythonOperator(
    task_id='generate_table',
    python_callable=generate_table,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=lambda: upload_to_s3(generate_task.output),
    dag=dag,
)

generate_task >> upload_task
