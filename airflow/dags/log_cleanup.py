from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime

# define default args
default_args = {
    'owner': 'airflow',
    'run_as_user': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 16),
    'email': ['ejoranlien@edanalytics.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'trigger_rule': 'all_success'
}

log_cleanup_dag = DAG('log_cleanup_dag', default_args=default_args, catchup=False, schedule_interval='@weekly')

# delete old scheduler logs to avoid disk bloat
delete_logs = """
find /home/airflow/airflow/logs/scheduler/ -mtime +5 -type d -exec rm -rf {} \;
"""

delete_old_logs = BashOperator(task_id='delete_old_logs',
                               bash_command=delete_logs,
                               dag=log_cleanup_dag)

