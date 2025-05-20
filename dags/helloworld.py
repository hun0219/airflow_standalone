from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'helloworld',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 5, 19),
    catchup=True,
    tags=['helloworld'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
#    t1 = BashOperator(
#        task_id='print_date',
#        bash_command='date',
#    )
#
#    t2 = BashOperator(
#        task_id='sleep',
#        depends_on_past=False,
#        bash_command='sleep 5',
#        retries=3,
#    )
#
#    t3 = DummyOperator(task_id='t3')
#    t33 = DummyOperator(task_id='t33')
#    t22 = DummyOperator(task_id='t32')
#    task_end = DummyOperator(task_id='end')
#    task_start = DummyOperator(task_id='start')
#    task_empty = DummyOperator(task_id='empty')

#    t1 >> [t2, t3, t22, t33] >> task_end
#    task_start >> t1
#    t3 >> task_empty
#    task_empty >> task_end

    start = DummyOperator(task_id='Visit_data')
    t1 = DummyOperator(task_id='Food')
    t2 = DummyOperator(task_id='Sights')
    t3 = DummyOperator(task_id='Hotel')
    end = DummyOperator(task_id='Result')

    start >> [t1,t2,t3] >> end
