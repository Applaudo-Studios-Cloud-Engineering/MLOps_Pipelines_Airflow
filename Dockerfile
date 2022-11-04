FROM apache/airflow:latest
USER airflow
COPY --chown=airflow:root ./classification_projects ./classification_projects
COPY requirements.txt requirements.txt 
RUN pip install -r requirements.txt
ENV PYTHONPATH "."