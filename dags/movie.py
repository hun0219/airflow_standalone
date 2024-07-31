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
    BranchPythonOperator,
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
    tags=['movie', 'api', 'amt'],
) as dag:


    def get_data(ds_nodash):
        #print(ds_nodash)
        #print(kwargs)
        #print("=" * 20)
        #print(f"ds_nodash =>{kwargs['ds_nodash']}")
        #print(f"kwargs type => {type(kwargs)}")
        #print("=" * 20)
        from mov.api.call import save2df
        #key = get_key()
        #print(f"MOVIE_API_KET => {key}")
        #YYYYMMDD = kwargs['ds_nodash'] #20240724
        df = save2df(ds_nodash)
        print(df.head(5))

    def save_data(ds_nodash):
        from mov.api.call import apply_type2df#get_key, echo
        df = apply_type2df(load_dt=ds_nodash)
        print( "*" * 33)
        print(df.head(10))
        print( "*" * 33)
        print(df.dtypes)
        
        #개봉일 기준 그룹핑 누적 관객수 합
        print("개봉일 기준 그룹핑 누적 관객수 합")
        g = df.groupby('openDt')
        sum_df = g.agg({'audiAcc':'sum'}).reset_index()
        print(sum_df)

        #key = get_key()
        #print( "*" * 33)
        #print(key)
        #msg = echo("hello")
        #print(msg)
        #print( "*" * 33)

#    def print_context(ds=None, **kwargs):
#        """Print the Airflow context and ds variable from the context."""
#        print("::group::All kwargs")
#        pprint(kwargs)
#        print(kwargs)
#        print("::endgroup::")
#        print("::group::Context variable ds")
#        print(ds)
#        print("::endgroup::")
#        return "Whatever you return gets printed in the logs"

    def branch_fun(ds_nodash):
        #ld = kwargs['ds_nodash']
        import os
        #OS의 경로 가져오는 방법
        home_dir = os.path.expanduser("~")
        path = f'{home_dir}/tmp/test_parquet/load_dt={ds_nodash}'
        #path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ld}")
        if os.path.exists(path):
            return "rm.dir" #task_id
        else:
            return "get.data", "echo.task" #task_id


    branch_op = BranchPythonOperator(
            task_id='branch.op',
            python_callable=branch_fun
            )

#    run_this = PythonOperator(
#            task_id="print_the_context", 
#            python_callable=print_context
#            )

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
    task_get_data = PythonVirtualenvOperator(
            task_id='get.data',
            python_callable=get_data,
            requirements=["git+https://github.com/hun0219/mov.git@0.3.0/api"],
            system_site_packages=False,
            #trigger_rule="none_failed"
            trigger_rule="all_done",
            #venv_cache_path="/home/hun/tmp2/airflow_venv/get_data"
            # venv_cache_path 물결 작동안함 풀패스, 디폴트는 지정 X아니면 none
            )
#trigger_rule = 'none_failed_min_one_success' 실패없이 둘중하나만 되면 OK

    task_save_data = PythonVirtualenvOperator(
            task_id='save.data',
            python_callable=save_data,
            requirements=["git+https://github.com/hun0219/mov.git@0.3.0/api"],
            system_site_packages=False,
            #trigger_rule="none_failed"
            trigger_rule="one_success",
            #venv_cache_path="/home/hun/tmp2/airflow_venv/get_data"
            # venv_cache_path 물결 작동안함 풀패스, 디폴트는 지정 X아니면 none
            )


#    task_save_data = BashOperator(
#            task_id='save.data',
#            bash_command='date',
#            #trigger_rule="all_done"
#            trigger_rule="one_success"
#            #trigger_rule="none_skipped"
#            #trigger_rule="always"
#            )

    rm_dir = BashOperator(
            task_id='rm.dir',
            bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}',
            )

    echo_task = BashOperator(
            task_id='echo.task',
            bash_command="echo 'task'"
            )


    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_join = BashOperator(
            task_id='task.join',
            bash_command="exit 1",
            trigger_rule="all_done"
            )



    task_start >> branch_op
    task_start >> task_join >> task_save_data

    branch_op >> rm_dir >> task_get_data
    branch_op >> echo_task >> task_save_data
    branch_op >> task_get_data
    
#    rm_dir >> task_join
#    echo_task >> task_join

#    task_join >> task_get_data
    

    task_get_data >> task_save_data >> task_end
