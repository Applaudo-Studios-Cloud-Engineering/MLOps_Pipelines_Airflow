# Latest Airflow release
FROM apache/airflow:latest

ENV PYTHONPATH "."

USER airflow

# required for docker approach, if using k8s comment the 2 copy instructions
COPY --chown=airflow:root ./submodules ./submodules
COPY --chown=airflow:root ./pipelines ./pipelines

COPY requirements.txt requirements.txt 

RUN pip install -r requirements.txt
