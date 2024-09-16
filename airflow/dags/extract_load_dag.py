
from airflow import DAG
from airflow.operators.python import PythonOperator

from dag_functions.extract_data import extract_education_data
from dag_functions.load_data import load_education_data
from dag_functions.af_util import xcom_pull_template

from datetime import datetime

resources = ['directory', 'enrollment', 'assessment']

# https://www.youtube.com/watch?v=-HXppV-UCv0
# https://www.youtube.com/watch?v=IH1-0hwFZRQ
dag = DAG(
    dag_id = "extract_load", 
    start_date = datetime(2024, 9, 13),
    schedule_interval = None,
    catchup = False
    ) 

for resource in resources: 
    extract = PythonOperator(
    task_id=f'extract_operation_for_{resource}',
    python_callable = extract_education_data,
    op_kwargs = {'resource': resource},
    dag = dag,
    do_xcom_push = True,
    provide_context = True
    )

    load = PythonOperator(
    task_id = f'load_education_data_for_{resource}',
    python_callable = load_education_data,
    op_kwargs = {
        'resource': resource, 
        'file_path': xcom_pull_template(task_ids=f'extract_operation_for_directory'),
    },
    dag = dag
    )

    extract >> load

