# Latest Airflow release
FROM apache/airflow:latest

ENV PYTHONPATH "."

USER airflow

COPY --chown=airflow:root ./submodules ./submodules

COPY requirements.txt requirements.txt 

RUN pip install -r requirements.txt