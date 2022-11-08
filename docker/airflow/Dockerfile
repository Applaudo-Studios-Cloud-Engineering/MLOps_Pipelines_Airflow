# Latest Airflow release
FROM apache/airflow:latest

ENV PYTHONPATH "."

USER airflow

COPY requirements.txt requirements.txt 

RUN pip install -r requirements.txt