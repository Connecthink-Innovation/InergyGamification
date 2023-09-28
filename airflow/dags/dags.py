from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator


MODE = 'preprod'

# ----- BASH COMMANDS
gamification_path = "~/Gamification"
gamification_executable = "~/Gamification/env/bin/python"

gamification_cmd = f'cd {gamification_path}; {gamification_executable} Gamification/main.py'

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'abelespin',
    'start_date': datetime(2023, 8, 7, 20, 30),  # Fecha y hora de inicio
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'gamification',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=default_args,
    description='execute gamification',
    schedule_interval=timedelta(days=1),
) as dag:
    task_gamification = BashOperator(
    task_id='execute_gamification',
    bash_command=gamification_cmd,
    )


task_gamification