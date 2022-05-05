#!Doc String
#last updated 5/4/2022
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
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

PIPELINE_DAG = DAG(
    dag_id='master_deploy_dag',
    template_searchpath=['/home/airflow/gcs/dags/'],
    catchup=False,
    start_date=datetime.datetime(year=2021, month=7, day=16),
    schedule_interval= '0 10 * * *',
    default_args = DEFAULT_DAG_ARGS,
    dagrun_timeout=timedelta(minutes=20)
)


#------------------------------------------------------------------#
START_OP = DummyOperator(task_id="START",
                         dag=PIPELINE_DAG)


database_deployment = TriggerDagRunOperator(
    task_id="trigger_database_deploy_dag",
    trigger_dag_id='database_deployment',
    dag=PIPELINE_DAG,
    wait_for_completion=True,
)

region_ingestion = TriggerDagRunOperator(
    task_id="trigger_region_ingestion_dag",
    trigger_dag_id='region_pipeline',
    dag=PIPELINE_DAG,
    wait_for_completion=True,
)

country_ingestion = TriggerDagRunOperator(
    task_id="trigger_country_ingestion_dag",
    trigger_dag_id='country_pipeline',
    dag=PIPELINE_DAG,
    wait_for_completion=True,
)

attack_ingestion = TriggerDagRunOperator(
    task_id="trigger_attack_ingestion_dag",
    trigger_dag_id='attack_pipeline',
    dag=PIPELINE_DAG,
    wait_for_completion=True,
)

weapon_ingestion = TriggerDagRunOperator(
    task_id="trigger_weapon_ingestion_dag",
    trigger_dag_id='weapon_pipeline',
    dag=PIPELINE_DAG,
    wait_for_completion=True,
)

group_ingestion = TriggerDagRunOperator(
    task_id="trigger_group_ingestion_dag",
    trigger_dag_id='group_pipeline',
    dag=PIPELINE_DAG,
    wait_for_completion=True,
)

target_ingestion = TriggerDagRunOperator(
    task_id="trigger_target_ingestion_dag",
    trigger_dag_id='target_pipeline',
    dag=PIPELINE_DAG,
    wait_for_completion=True,
)

event_ingestion = TriggerDagRunOperator(
    task_id="trigger_event_ingestion_dag",
    trigger_dag_id='event_pipeline',
    dag=PIPELINE_DAG,
    wait_for_completion=True,
)

END_OP = DummyOperator(task_id="FINISH")

START_OP >> database_deployment 
database_deployment >> [region_ingestion, country_ingestion]
region_ingestion >> [group_ingestion, weapon_ingestion, attack_ingestion]
country_ingestion >> [group_ingestion, weapon_ingestion, attack_ingestion]
[group_ingestion, weapon_ingestion, attack_ingestion] >>  target_ingestion >> event_ingestion >> END_OP
