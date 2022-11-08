from pipelines.titanic import create_preprocessing_pipeline, create_ml_pipeline, create_feature_engineering_pipeline, prepare_submission
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
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
    dag_id="titanic-challenge-kubernetes-pod",
    schedule_interval="@daily",
    start_date=datetime(2022, 10, 1),
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60)
)

create_preprocessing = KubernetesPodOperator(dag=dag, 
    task_id='create_preprocessing',
    cmds=["python3", "-c"],
    arguments=["from pipelines.titanic import create_preprocessing_pipeline; create_preprocessing_pipeline('submodules/classification_projects/titanic_challenge/data/train.csv', True)"],
    executor_config = resource_config,
    namespace="default",
    image="ghcr.io/anggutie-dev/dags:latest",
    name="create-preprocessing-pod",
    in_cluster=True,
    is_delete_operator_pod=True,
    get_logs=True
)

create_feature_engineering = KubernetesPodOperator(dag=dag, 
    task_id='create_feature_engineering',
    cmds=["python3", "-c"],
    arguments=["from pipelines.titanic import create_feature_engineering_pipeline; create_feature_engineering_pipeline('/tmp/clean.csv')"],
    executor_config = resource_config,
    namespace="default",
    image="ghcr.io/anggutie-dev/dags:latest",
    name="create-feature-engineering-pod",
    in_cluster=True,
    is_delete_operator_pod=True,
    get_logs=True
)

create_preprocessing >> create_feature_engineering
