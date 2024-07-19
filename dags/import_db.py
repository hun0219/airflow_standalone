from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
    'import_db',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='import db',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['import', 'db', 'import db'],
) as dag:




    task_check = BashOperator(
            task_id="check",
            bash_command="bash {{ var.value.CHECK_SH }} {{ds_nodash}}"
#            echo "check"
#            DONE_PATH=~/data/done/{{ds_nodash}}
#            DONE_PATH_FILE="${DONE_PATH}/_DONE"
#
#            #파일 존재 여부 확인
#            if [ -e "$DONE_PATH_FILE" ]; then
#                figlet "Let's move on"
#                exit 0
#            else
#                echo "I'll be back => $DONE_PATH_FILE"
#                exit 1
#            fi
    )

    task_csv = BashOperator(
            task_id="to.csv",
            bash_command="""
            echo "to.csv"
            U_PATH=~/data/count/{{ds_nodash}}/count.log
            CSV_PATH=~/data/csv/{{ds_nodash}}
#            CP_PATH=~/data/csv/{{ds_nodash}}/csv.csv

            mkdir -p $CSV_PATH
            cat $U_PATH | awk '{print "{{ds}}," $2 "," $1}' > ${CSV_PATH}/csv.csv


            """
    )

    task_tmp = BashOperator(
            task_id="to.tmp",
            bash_command="""
            """
    )

    task_base =  BashOperator(
            task_id="to.base",
            bash_command="""
            """
    )

    task_done = BashOperator(
            task_id="make.done",
            bash_command="""
            """
    )

    task_err = BashOperator(
            task_id="err.report",
            bash_command="""
                echo "err report"
            """,
            trigger_rule="one_failed"
    )

    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")

    task_start >> task_check >> task_csv >> task_tmp >> task_base >> task_done
    task_done >> task_end

    task_check >> task_err >> task_end
