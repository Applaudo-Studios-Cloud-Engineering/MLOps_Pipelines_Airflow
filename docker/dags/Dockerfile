# Latest Airflow release
FROM python:latest

ENV PYTHONPATH "."

WORKDIR /dags

COPY --chown=airflow:root ./submodules ./submodules
COPY --chown=airflow:root ./pipelines ./pipelines
COPY --chown=airflow:root ./requirements.txt ./requirements.txt

RUN pip install -r requirements.txt
