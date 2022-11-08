from pipelines.titanic import create_preprocessing_pipeline, create_ml_pipeline, create_feature_engineering_pipeline, prepare_submission
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


default_args = {
    'owner': 'Angel Gutierrez',
    'email': ['agutierrez@applaudostudios.com']
}


resource_config = {
    "KubernetesExecutor": {
        "request_memory": "200Mi",
        "limit_memory": "200Mi",
        "request_cpu": "200m",
        "limit_cpu": "200m"
    }
}


dag = DAG(
    dag_id="titanic-challenge-operator",
    schedule_interval="@daily",
    start_date=datetime(2022, 10, 1),
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60)
)

create_preprocessing = PythonOperator(dag=dag, 
    task_id='create_preprocessing', 
    python_callable=create_preprocessing_pipeline, 
    op_kwargs={"dataset_path": "/opt/airflow/dags/repo/submodules/classification_projects/titanic_challenge/data/train.csv", "drop_passenger_id": True},
    provide_context=True,
    executor_config = resource_config
)

create_feature_engineering = PythonOperator(
    dag=dag, 
    task_id='create_feature_engineering', 
    python_callable=create_feature_engineering_pipeline,
    provide_context=True,
    op_kwargs={"dataset_path": "/tmp/clean.csv"},
    executor_config = resource_config
)

create_ml = PythonOperator(
    dag=dag, 
    task_id='create_ml', 
    python_callable=create_ml_pipeline,
    provide_context=True,
    op_kwargs={"dataset_path": "/tmp/feature.csv"},
    executor_config = resource_config
)

prepare_submission = PythonOperator(
    dag=dag, 
    task_id='prepare_submission_pipeline', 
    python_callable=prepare_submission,
    op_kwargs={'test_df_path': '/opt/airflow/dags/repo/submodules/classification_projects/titanic_challenge/data/test.csv', 'clean_df_path': '/tmp/clean.csv', 'feature_df_path': '/tmp/feature.csv', 'submission_file_path': '/tmp/submission.csv'},
    executor_config = resource_config
)

create_preprocessing >> create_feature_engineering >> create_ml >> prepare_submission