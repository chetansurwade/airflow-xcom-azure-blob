from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from projects.dummy_project import main


with DAG('dummy_project_dag', description='Python DAG for capability demostration', schedule_interval='*/10 * * * *', start_date=datetime.today(), catchup=False) as dag:
    python_task1 = PythonOperator(
        task_id='python_task1', python_callable=main.print_func1)
    python_task2 = PythonOperator(
        task_id='python_task2', python_callable=main.print_func2)
    python_task3 = PythonOperator(
        task_id='python_task3', python_callable=main.print_func3)
    [python_task1 >> python_task2] >> python_task3
