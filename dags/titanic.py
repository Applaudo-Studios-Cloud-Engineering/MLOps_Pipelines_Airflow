from datetime import timedelta, datetime
from multiprocessing.spawn import prepare
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import pickle

from classification_projects.classification_projects.titanic_challenge.src.nodes import create_dataset, drop_unnecessary_columns, fill_empty_age_values, fill_empty_embarked_values, \
    encode_embarked_ports, create_deck_feature, encode_age_ranges, create_title_feature, encode_title_feature, \
    encode_sex, encode_fare, create_age_class_feature, create_relatives_feature, split_dataset_for_training, \
    create_and_train_decision_tree_model, compute_accuracy, fill_empty_fare_values



def create_preprocessing_pipeline(dataset_path: str, drop_passenger_id: bool=False) -> pd.DataFrame:
    df = create_dataset(dataset_path)

    if drop_passenger_id:
        df = drop_unnecessary_columns(df, ['PassengerId'])

    df = fill_empty_age_values(df)

    df = fill_empty_embarked_values(df)

    df = fill_empty_fare_values(df)

    df.to_csv('/tmp/clean.csv')


def create_feature_engineering_pipeline(dataset_path: str):
    df = pd.read_csv(dataset_path)

    df = create_deck_feature(df, False)

    df = create_title_feature(df)

    sexes = {"male": 0, "female": 1}

    df = encode_sex(df, sexes)

    df = create_relatives_feature(df, False)

    df = drop_unnecessary_columns(df, ['Cabin', 'Name', 'Ticket', 'SibSp', 'Parch'])

    df = encode_embarked_ports(df)

    df = encode_fare(df)

    df = encode_age_ranges(df)

    titles = {"Mr": 1, "Miss": 2, "Mrs": 3, "Master": 4, "Rare": 5}

    df = encode_title_feature(df, titles)

    df = create_age_class_feature(df)

    df.to_csv('/tmp/feature.csv')


def create_ml_pipeline(dataset_path: str):
    train_df = pd.read_csv(dataset_path)

    X_train, Y_train = split_dataset_for_training(train_df, 'Survived')

    model = create_and_train_decision_tree_model(X_train, Y_train)

    training_acc = compute_accuracy(model, X_train, Y_train)
    
    pickle.dump(model, open(f'/tmp/dt_classifier_acc', 'wb'))

    return training_acc


def prepare_submission(test_df_path, clean_df_path, feature_df_path, submission_file_path):
    model = pickle.load(open('/tmp/dt_classifier_acc', 'rb'))

    create_preprocessing_pipeline(test_df_path, False)

    create_feature_engineering_pipeline(clean_df_path)

    clean_df = pd.read_csv(feature_df_path)

    X_test = drop_unnecessary_columns(clean_df, ['PassengerId'])

    Y_pred = model.predict(X_test)

    data = {'PassengerId': clean_df['PassengerId'], 'Survived': Y_pred}

    submission_df = pd.DataFrame(data)

    submission_df.to_csv(submission_file_path)

    return submission_df


default_args = {
    'owner': 'Angel Gutierrez'
}


dag = DAG(
    dag_id="titanic-challenge",
    schedule_interval="@daily",
    start_date=datetime(2022, 10, 1),
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60)
)

create_preprocessing = PythonOperator(dag=dag, 
    task_id='create_preprocessing', 
    python_callable=create_preprocessing_pipeline, 
    op_kwargs={"dataset_path": "/opt/airflow/classification_projects/classification_projects/titanic_challenge/data/train.csv", "drop_passenger_id": True},
    provide_context=True
)

create_feature_engineering = PythonOperator(
    dag=dag, 
    task_id='create_feature_engineering', 
    python_callable=create_feature_engineering_pipeline,
    provide_context=True,
    op_kwargs={"dataset_path": "/tmp/clean.csv"}
)

create_ml = PythonOperator(
    dag=dag, 
    task_id='create_ml', 
    python_callable=create_ml_pipeline,
    provide_context=True,
    op_kwargs={"dataset_path": "/tmp/feature.csv"}
)

prepare_submission = PythonOperator(
    dag=dag, 
    task_id='prepare_submission_pipeline', 
    python_callable=prepare_submission,
    op_kwargs={'test_df_path': '/opt/airflow/classification_projects/classification_projects/titanic_challenge/data/test.csv', 'clean_df_path': '/tmp/clean.csv', 'feature_df_path': '/tmp/feature.csv', 'submission_file_path': '/tmp/submission.csv'}
)

create_preprocessing >> create_feature_engineering >> create_ml >> prepare_submission