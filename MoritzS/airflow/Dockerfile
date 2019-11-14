# Use an official Python runtime as a parent image
FROM puckel/docker-airflow

WORKDIR /src
COPY requirements.txt /src

COPY /src/ /usr/local/airflow/dags
