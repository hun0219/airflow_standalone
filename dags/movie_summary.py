from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint as pp

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
    start_date=datetime(2025, 5, 19),
    catchup=True,
    tags=['movie', 'api', 'amt', 'summary'],
) as dag:
    REQUIREMENTS=["git+https://github.com/hun0219/mov.git@0.3.0/api"]



    def gen_empty(*ids):
        #def gen_empty(id):
        tasks = [] #튜플() <- 은 변경이 어려움, list사용
        for id in ids: #튜플을 리스트 돌면서 생성
            task = EmptyOperator(task_id=id)
            tasks.append(task)
        return tuple(tasks) #튜플(t, ) 리턴

    #def gen_vpython(id, fun_obj, op_kwargs):
    def gen_vpython(**kw):
        # {'abc' : app~~ , aaa : {key : value}}
        #id = kw['id']
        #fun_o = kw['fun_obj']
        #op_kw = kw['op_kwargs']
        task = PythonOperator(
            task_id=kw['id'],
            python_callable=kw['fun_obj'],
            #system_site_packages=False,
            #requirements=REQUIREMENTS,
            op_kwargs=kw['op_kw'] # opkw -> opkwargs파라미터에 저장
#            op_kwargs={
#                "url_param": {"multiMovieYn":"Y"},
#                "ds":"2023-11-11",
#                "ds_nodash":"20231101"
#                }
            )
        return task


    #def pro_data(ds_nodash, arg1):
    def pro_data(**params): #키워드 아규먼트 전체 받는거(**params)
        ####pro_data(ds_~, **params)
        ### 앞에 ds_nodash 선언하면 **params안에 ds_~ 빠진다.
        # kwargs -> {ds_nodash : 240712} <- op_kw 키 벨류 들어가
        # {ds_no~~ : 20521, aaa : {key: value}}
        # params안에 opkwargs 값 있음 param 안에 aaa 키호출 가능
        #print(f"{ds_nodash}")
        #print(f"{params['key']}")
        #print(f"{params}")
        #print(f"{params['arg1']}")
        #print(f"{params['abc']}, {type(params['abc'])}")
        #print("pro data")
        print("@" *33)
        print(params['task_name'])  #여기는 출력시 task_name
        print(params)
        pp(params)
        print("@" *33)

    def pro_data2(task_name, **params):
        print("@" *33)
        print(task_name)
        print(params) # 여기는 출력시 task_name 없음 
        pp(params) # 여기는 출력시 task_name 없음 
        if "task_name" in params.keys():
            print("========================있음")
        else:
            print("========================없음")
        print("@" *33)

    def pro_data3(task_name):
        print("@" *33)
        print(task_name)
        #print(params) # 여기는 출력시 task_name 없음
        print("@" *33)

    def pro_data4(task_name, ds_nodash, **kwargs):
        print("@" *33)
        print(task_name)
        print(ds_nodash)
        print(kwargs) # 여기는 출력시 task_name, ds_nobash 없음
        print("@" *33)

    
##############################################################    
    start, end = gen_empty('start', 'end')
    
    apply_type = gen_vpython(
            id = "apply.type",
            fun_obj = pro_data,
            op_kw={ "task_name": "apply_type!"}
            )

    merge_df = gen_vpython(
            id = "merge.df",
            fun_obj = pro_data2,
            op_kw={ "task_name": "merge_df!" }
            )

    de_dup = gen_vpython(
            id = "de.dup",
            fun_obj = pro_data3,
            op_kw={ "task_name": "de_dup!" }
            )

    summary_df = gen_vpython(
            id = "summary.df",
            fun_obj = pro_data4,
            op_kw={ "task_name": "summary_df!" }
            )


    #start = EmptyOperator(task_id='start')
    #end = EmptyOperator(task_id='end', trigger_rule="all_done")

##################################################################

    start >> apply_type >> merge_df >> de_dup >> summary_df >> end

