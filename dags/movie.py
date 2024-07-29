from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
    PythonVirtualenvOperator,
)

with DAG(
    'movie',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='move',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'api'],
) as dag:


    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print("=" * 20)
        print(f"ds_nodash =>{kwargs['ds_nodash']}")
        print(f"kwargs type => {type(kwargs)}")
        print("=" * 20)
        from mov.api.call import get_key, save2df
        key = get_key()
        print(f"MOVIE_API_KET => {key}")
        YYYYMMDD = kwargs['ds_nodash'] #20240724
        df = save2df(YYYYMMDD)

    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print("::group::All kwargs")
        pprint(kwargs)
        print(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"


    run_this = PythonOperator(
            task_id="print_the_context", 
            python_callable=print_context)

#    task_check = BashOperator(
#            task_id="check",
#            bash_command="bash {{ var.value.CHECK_SH }} {{ds_nodash}}"
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
#    )
    task_gat_data = PythonVirtualenvOperator(
            task_id="gat_data",
            python_callable=get_data,
            requirements=["git+https://github.com/hun0219/mov.git@0.2.0/api"],
            system_site_packages=False,
            )

    task_save_data = BashOperator(
            task_id="save_data",
            bash_command="""
                echo "save.data"
            """
            )


    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")

    task_start >> task_gat_data >> task_save_data >> task_end
    task_start >> run_this >> task_end
