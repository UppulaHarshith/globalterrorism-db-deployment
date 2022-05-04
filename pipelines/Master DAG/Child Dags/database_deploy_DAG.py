import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

date = datetime.now().strftime("%Y%m%d%I%M%S")

PROJECT_ID='dev-world-terrorism-analytics'


from datetime import timedelta

DEFAULT_DAG_ARGS = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'project_id': PROJECT_ID
}


import datetime 

database_build = DAG(  
    dag_id = 'database_deploy',
    template_searchpath=['/home/airflow/gcs/dags/'],
    catchup = False,
    start_date=datetime.datetime(year=2021, month=7, day=16),
    schedule_interval= '0 10 * * *',
    default_args = DEFAULT_DAG_ARGS,
    dagrun_timeout=timedelta(minutes=20)
)

START_OP = DummyOperator(task_id="START",
                         dag=database_build)




#The following operator applies transformation.sql script and pushes the data to a validation table
database_dev_pipeline = BashOperator(
    task_id='database_deployment',
    wait_for_downstream=False,
    bash_command='./main.sh',
    dag=database_build)


# validation_data_ingest_dag_pipeline
START_OP.set_downstream(database_dev_pipeline)