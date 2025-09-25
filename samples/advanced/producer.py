# Domine Apache Airflow. https://www.eia.ai/
from airflow import DAG
from airflow import Dataset
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd


dag =  DAG('producer', description="producer",
        schedule_interval=None,start_date=datetime(2023,3,5),
        catchup=False)

mydataset = Dataset("/opt/airflow/data/table_monitored.csv")

def my_file():
    dataset = pd.read_csv("/opt/airflow/data/table.csv", sep=";")
    dataset.to_csv("/opt/airflow/data/table_monitored.csv", sep=";")

producer1 = PythonOperator(task_id='Convert_to_csv', 
                    python_callable=my_file,
                    dag=dag,
                    outlets=[mydataset])
producer1



