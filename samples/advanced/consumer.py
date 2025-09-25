# Domine Apache Airflow. https://www.eia.ai/
from airflow import DAG
from airflow import Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

mydataset = Dataset("/opt/airflow/data/table_monitored.csv")

dag =  DAG('consumer', description="consumer",
        schedule=[mydataset],start_date=datetime(2025,9,25),
        catchup=False)

def update_file():
    dataset = pd.read_csv("/opt/airflow/data/table_monitored.csv", sep=";")
    found = dataset.loc[(dataset['X4']<0) | (dataset['X4']> 120), 'X4']
    print(found)
    dataset.to_csv("/opt/airflow/data/table_updated.csv", sep=";")

consumer1 = PythonOperator(task_id='updating_table', 
                           python_callable=update_file,
                           dag=dag,
                           provide_context=True
                           )
consumer1