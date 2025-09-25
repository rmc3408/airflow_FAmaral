# Domine Apache Airflow. https://www.eia.ai/
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

dag =  DAG('primeira_dag', description="Nossa primeira DAG",
        schedule_interval=None,start_date=datetime(2025,9,25),
        catchup=False)

task1 = BashOperator(task_id="tsk1",bash_command="sleep 3",dag=dag )
task2 = BashOperator(task_id="tsk2",bash_command="sleep 3",dag=dag )
task3 = BashOperator(task_id="tsk3",bash_command="sleep 3",dag=dag )

task1 >> task2 >> task3