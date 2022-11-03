from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.papermill_operator import PapermillOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Angel Gutierrez'
}

with DAG(
    dag_id='tweeter',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 10, 1),
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    # [START howto_operator_papermill]
    run_this = PapermillOperator(
        task_id="event_classification",
        input_nb="notebooks/tweets_event_classification/solution.ipynb",
        output_nb="/tmp/out-{{ execution_date }}.ipynb",
        parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"}
    )
