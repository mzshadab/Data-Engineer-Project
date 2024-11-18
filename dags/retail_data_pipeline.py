from airflow import DAG
from airflow.operator.bash import BashOperator
from datetime import datetime,timedelta

default_args = {
        'owner' = 'airflow',
        'retries' = 1,
        'retry_delay' = timedelta(minutes=2)
}

dag = DAG(
        dag_id = 'retail_data_pipeline',
        default_args = default_args,
        start_date = datetime(2024,1,1),
        schedule_interval='@daily'
)

extract_data = BashOperator(
        task_id='extract_data',
        bash_command='python3 /home/hdoop/retail_project/extract_data.py',
        dag = dag
)

transform_data = BashOperator(
        task_id = 'transform_data',
        bash_command = 'spark-submit /home/hdoop/retail_project/transformed_data.py',
        dag = dag
)

load_data = BashOperator(
        task_id = 'load_data',
        bash_command = 'hive -f /home/hdoop/retail_project/load_data.hql',
        dag = dag
)

# tasks dependencies

extract_data >> transform_data >> load_data

