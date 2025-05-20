from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

def gen_emp(id, rule="all_success"):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
    'make_parquet',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=2,
    description='parquet db',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 5, 19),
    catchup=True,
    tags=['parquet', 'make parquet'],
) as dag:




    task_check = BashOperator(
            task_id="check.done",
            bash_command="""
                DONE_FILE={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}/_DONE
                #DONE_FILE=/home/hun/done/import/20240715/_DONE
                bash {{ var.value.CHECK_PAR }} $DONE_FILE

                #bash {{ var.value.CHECK_PAR }} {{ds_nodash}}"
            """
    )

    task_parquet = BashOperator(
            task_id="to.parquet",
            bash_command="""
                echo "to.parquet"

                READ_PATH="~/data/csv/{{ds_nodash}}/csv.csv"
                SAVE_PATH="~/data/parquet/"

                echo "***********************"
                echo $SAVE_PATH
                #mkdir -p $SAVE_PATH
                echo $SAVE_PATH
                echo "***********************"

                python {{ var.value.CSV2PAR }} $READ_PATH $SAVE_PATH
            """
    )

    task_done = BashOperator(
            task_id="make.done",
            bash_command="""
                echo "make.done"
                figlet "make.done.start"

                DONE_PATH={{ var.value.PAR_DONE_PATH }}/{{ds_nodash}}
                mkdir -p $DONE_PATH
                echo "PAR_DONE_PATH=$DONE_PATH"
                touch $DONE_PATH/_DONE

                figlet "make.done.end"
            """
    )

    task_err = BashOperator(
            task_id="err.report",
            bash_command="""
                echo "err report"
            """,
            trigger_rule="one_failed"
    )

    task_start = gen_emp('start')
    task_end = gen_emp('end', 'all_done')
    
    task_start >> task_check
    task_check >> task_parquet >> task_done >> task_end
    task_check >> task_err >> task_end
