FROM apache/airflow:latest
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-papermill ipykernel
#RUN mkdir -p notebooks
COPY --chown=default:root ./classification_projects ./classification_projects
#COPY classification_projects/tweets_event_classification ./notebooks/tweets_event_classification
#COPY classification_projects/titanic_challenge ./notebooks/titanic_challenge
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
ENV PYTHONPATH "."