import pandas as pd
import sqlite3
import os
import zipfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from kaggle.api.kaggle_api_extended import KaggleApi

# Default arguments for the DAG
default_args = {
    'owner': 'quynhanh',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'uci_heart_disease_etl',
    default_args=default_args,
    description='ETL pipeline for UCI Heart Disease Dataset',
    schedule_interval='@daily',
)

# File paths
dataset_dir = '/home/quynhanh_intro/airflow/datasets'
dataset_path = os.path.join(dataset_dir, 'heart.csv')
transformed_path = os.path.join(dataset_dir, 'transformed_heart_disease.csv')
db_path = '/home/quynhanh_intro/airflow/databases/heart_disease.db'

# Task 1: Extract data
def extract_data(**kwargs):
    os.environ['KAGGLE_CONFIG_DIR'] = '/home/quynhanh_intro/.kaggle'
    
    # Set up Kaggle API
    api = KaggleApi()
    api.authenticate()

    # Download the dataset
    api.dataset_download_file('uciml/heart-disease-uci', file_name='heart.csv', path=dataset_dir)

    # Check if the downloaded file is a ZIP file
    zip_file_path = dataset_path + '.zip'
    if zipfile.is_zipfile(zip_file_path):
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(dataset_dir)
        os.remove(zip_file_path)

    # Push dataset path to XCom
    kwargs['ti'].xcom_push(key='dataset_path', value=dataset_path)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Transform data
def transform_data(**kwargs):
    # Retrieve dataset path from XCom
    dataset_path = kwargs['ti'].xcom_pull(key='dataset_path')
    df = pd.read_csv(dataset_path)

    # Handle missing values
    df['ca'] = df['ca'].replace('?', pd.NA).astype(float).fillna(df['ca'].mode()[0])
    df['thal'] = df['thal'].replace('?', pd.NA).astype(float).fillna(df['thal'].mode()[0])

    # Create a new column 'risk_level'
    def risk_level(chol):
        if chol < 200:
            return 'Low risk'
        elif 200 <= chol <= 240:
            return 'Medium risk'
        else:
            return 'High risk'

    df['risk_level'] = df['chol'].apply(risk_level)

    # Save transformed data
    df.to_csv(transformed_path, index=False)
    kwargs['ti'].xcom_push(key='transformed_path', value=transformed_path)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Task 3: Load data
def load_data(**kwargs):
    transformed_path = kwargs['ti'].xcom_pull(key='transformed_path')
    df = pd.read_csv(transformed_path)

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS heart_disease_data (
            id INTEGER,
            age INTEGER,
            sex INTEGER,
            dataset TEXT,
            cp INTEGER,
            trestbps REAL,
            chol REAL,
            fbs INTEGER,
            restecg INTEGER,
            thalch INTEGER,
            exang INTEGER,
            oldpeak REAL,
            slope INTEGER,
            ca INTEGER,
            thal INTEGER,
            num INTEGER,
            risk_level TEXT
        )
    ''')

    # Insert transformed data into the database
    df.to_sql('heart_disease_data', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
