FROM apache/airflow:latest
USER airflow
COPY --chown=airflow:root ./submodules ./submodules
COPY requirements.txt requirements.txt 
RUN pip install -r requirements.txt
ENV PYTHONPATH "."