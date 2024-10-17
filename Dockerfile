FROM apache/airflow:2.4.0

USER root
RUN apt-get update && apt-get install -y awscli

USER airflow

# Install necessary Python packages
RUN pip install boto3 pandas

# Copy the DAGs
COPY ./dags /opt/airflow/dags
