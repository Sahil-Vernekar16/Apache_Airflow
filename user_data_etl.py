import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os

# JSONPlaceholder API endpoint for users
API_URL = 'https://jsonplaceholder.typicode.com/users'

# Function to extract data from JSONPlaceholder API
def extract_user_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()
        # Save the raw data for reference or further use
        with open("/home/sahilv/airflow/dags/user_data.json", "w") as f:
            json.dump(data, f)
        return data
    else:
        raise Exception("Failed to fetch data from JSONPlaceholder API")

# Function to transform the user data
def transform_user_data(ti):
    # Pull data from the extract task (extract_user_data)
    data = ti.xcom_pull(task_ids='extract_user_data')
    
    if data:
        # Extract and structure data (you can expand based on your needs)
        transformed_data = []
        for user in data:
            user_info = {
                'id': user['id'],
                'name': user['name'],
                'username': user['username'],
                'email': user['email'],
                'phone': user['phone'],
                'website': user['website'],
                'company': user['company']['name'],
            }
            transformed_data.append(user_info)

        # Convert to DataFrame
        df = pd.DataFrame(transformed_data)

        # Save data as CSV (Append if file exists)
        if os.path.exists("/home/sahilv/airflow/dags/user_data.csv"):
            df.to_csv('/home/sahilv/airflow/dags/user_data.csv', mode='a', header=False, index=False)
        else:
            df.to_csv('/home/sahilv/airflow/dags/user_data.csv', index=False)
        
        return transformed_data
    else:
        raise Exception("No data available for transformation")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'user_data_etl',
    default_args=default_args,
    description='An ETL pipeline to fetch user data from JSONPlaceholder API',
    schedule_interval='@daily',  # Schedule it to run daily
    start_date=datetime(2024, 12, 4),
    catchup=False,
) as dag:
    
    # Task 1: Extract user data from JSONPlaceholder API
    extract_user_data_task = PythonOperator(
        task_id='extract_user_data',
        python_callable=extract_user_data,
    )

    # Task 2: Transform the user data and save it to CSV
    transform_user_data_task = PythonOperator(
        task_id='transform_user_data',
        python_callable=transform_user_data,
        provide_context=True,  # To use XCom to pass data between tasks
    )

    # Set task dependencies
    extract_user_data_task >> transform_user_data_task
