from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
    PythonVirtualenvOperator,
    BranchPythonOperator,
)

with DAG(
    'movie_summary',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_tasks=3,
    max_active_runs=1,
    description='move',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'api', 'amt'],
) as dag:

    apply_type = BashOperator(
            task_id='apply.type',
            bash_command="""
            echo "apply_type"
            """,
            )

    merge_df = BashOperator(
            task_id='merge.df',
            bash_command="""
            echo "merge_df"
            """,
            )

    de_dup = BashOperator(
            task_id='de_dup',
            bash_command="""
            echo "de_dup"
            """,
            )

    summary_df = BashOperator(
            task_id='summary.df',
            bash_command="""
            echo "summary_df"
            """,
            )


    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")

##################################################################

    task_start >> apply_type >> merge_df >> de_dup >> summary_df >> task_end

