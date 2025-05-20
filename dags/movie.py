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
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1, #태스크가 실패했을때 다시시도 횟수
        'retry_delay': timedelta(seconds=3),
    },
    max_active_tasks=3,
    max_active_runs=1,
    description='move',
    schedule="10 2 * * *",
    start_date=datetime(2025, 5, 19),
    catchup=True,
    tags=['movie', 'api', 'amt'],
) as dag:

 
    def common_get_data(ds_nodash, arg1):
    #def common_get_data(ds_nodash, {"mult_4key":"Y}):
        from mov.api.call import save2df
        df = save2df(load_dt=ds_nodash, url_param=arg1)
        print(arg1)
        print(df[['movieCd', 'movieNm']].head(5))

        for key, value in arg1.items():
            df[key] = value
            
        #p_cols = list(url_param.keys()).insert(0, 'load_dt')
        p_cols = ['load_dt'] + list(arg1.keys())
        df.to_parquet('~/tmp/test_parquet', 
                partition_cols=p_cols
                # partition_cols=['load_dt', 'movieKey']
        )


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


    def branch_fun(ds_nodash):
        #ld = kwargs['ds_nodash']
        import os
        #OS의 경로 가져오는 방법
        home_dir = os.path.expanduser("~")
        path = f'{home_dir}/tmp/test_parquet/load_dt={ds_nodash}'
        #path = os.path.join(home_dir, f"tmp/test_parquet/load_dt={ld}")
        if os.path.exists(path):
            return "rm.dir" #task_id #rm_dor.task_id 도 가능
        else:
            return "get.start", "echo.task" #task_id


    branch_op = BranchPythonOperator(
            task_id='branch.op',
            python_callable=branch_fun
            )

##trigger_rule = 'none_failed_min_one_success' 실패없이 둘중하나만 되면 OK

    task_save_data = PythonVirtualenvOperator(
            task_id='save.data',
            python_callable=save_data,
            system_site_packages=False,
            #trigger_rule="none_failed"
            requirements=["git+https://github.com/hun0219/mov.git@0.3.0/api"],
            trigger_rule="one_success",
            #venv_cache_path="/home/hun/tmp2/airflow_venv/get_data"
            # venv_cache_path 물결 작동안함 풀패스, 디폴트는 지정 X아니면 none
            )

     #multi_y = EmptyOperator(task_id='multi.y') # 다양성 영화 유무

     # 다양성 영화 유무
    multi_y = PythonVirtualenvOperator(
            task_id='multi.y',
            python_callable=common_get_data,
            system_site_packages=False,
            op_kwargs={"arg1":{"multiMovieYn":"Y"}},
            requirements=["git+https://github.com/hun0219/mov.git@0.3.0/api"],
            )

    multi_n = PythonVirtualenvOperator(
            task_id='multi.n',
            python_callable=common_get_data,
            system_site_packages=False,
            op_kwargs={"arg1":{"multiMovieYn":"N"}},
            requirements=["git+https://github.com/hun0219/mov.git@0.3.0/api"],
            )
    # 한국 외국 영화
    nation_k = PythonVirtualenvOperator(
            task_id='nation.k',
            python_callable=common_get_data,
            system_site_packages=False,
            op_kwargs={"arg1":{"repNationCd":"K"}},
            requirements=["git+https://github.com/hun0219/mov.git@0.3.0/api"],
            )

    nation_f = PythonVirtualenvOperator(
            task_id='nation.f',
            python_callable=common_get_data,
            system_site_packages=False,
            op_kwargs={"arg1":{"repNationCd":"F"}},
            requirements=["git+https://github.com/hun0219/mov.git@0.3.0/api"],
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

    #multi_y = EmptyOperator(task_id='multi.y') # 다양성 영화 유무
    #multi_n = EmptyOperator(task_id='multi.n')
    #nation_k = EmptyOperator(task_id='nation_k') # 한국외국영화
    #nation_f = EmptyOperator(task_id='nation_f')   
    
    get_start = EmptyOperator(task_id='get.start', trigger_rule="all_done")
    get_end = EmptyOperator(task_id='get.end', trigger_rule="all_done")

    throw_err = BashOperator(
            task_id='throw.err',
            bash_command="exit 1",
            trigger_rule="all_done"
            )
##################################################################    

    task_start >> branch_op
    task_start >> throw_err >> task_save_data

    branch_op >> [rm_dir, echo_task] >> get_start
    branch_op >> get_start

    get_start >> [multi_y, multi_n, nation_k, nation_f]
    [multi_y, multi_n, nation_k, nation_f] >> get_end

    get_end >> task_save_data >> task_end

#    rm_dir >> throw_err
#    echo_task >> throw_err
#    throw_err >> task_get_data
    

