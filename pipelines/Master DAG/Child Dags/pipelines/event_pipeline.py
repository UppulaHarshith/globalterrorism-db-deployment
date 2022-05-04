import os
os.system("sudo pip3 install pymysql")
os.system("sudo pip3 install sqlalchemy")
os.system("sudo pip3 install sqlalchemy_utils")
os.system("sudo pip3 install cloud-sql-python-connector[pymysql]")
os.system("sudo pip3 install google-cloud-bigquery")
os.system("sudo pip3 install google-cloud-error-reporting")
os.system("sudo pip3 install pandas")
os.system("sudo pip3 install --upgrade google-cloud-logging")
os.system("sudo pip3 install pandas-gbq")

#all import statements
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import logging_v2
from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
from datetime import date
import pandas as pd
import datetime as dt
import datetime, logging
import pandas as pd
import pymysql
import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import numpy as np
from google.cloud.sql.connector import Connector
import sqlalchemy

today = date.today()
PROJECT_ID = 'dev-world-terrorism-analytics'
TABLE_NAME = 'event_dwh'
DATASET_ID = 'global_terrorism_dwh_dev'
rdb_instance = 'terrorism-instance'
rdb_region = 'us-central1'
rdb_user = 'root'
rdb_pwd = 'root'
rdb_db = 'gtd_database'

class LoggingHandler:
    """custom logger implementation to log Messages at different severity levels"""
    def __init__(self, project, log_name, trace):
        self.project = project
        self.log_name = 'projects/{}/logs/{}'.format(project, log_name)
        self.trace = trace
        self.client = LoggingServiceV2Client()
        # self.log_name = log_name
        self.resource = {
            "type": "global",
            "labels": {
                "project_id": self.project
            }
        }

    def _log_msg(self, Message, severity):
        entries = [{
            'severity': severity
        }]
        if isinstance(Message, dict):
            msg = Struct()
            msg.update(Message)
            entries[0]['json_payload'] = msg
        else:
            entries[0]['text_payload'] = Message

        self.client.write_log_entries(
            entries=entries,
            log_name=self.log_name,
            resource=self.resource)

    def info(self, Message):
        self._log_msg(Message, severity='INFO')

    def error(self, Message):
        self._log_msg(Message, severity='ERROR')

    def critical(self, Message):
        self._log_msg(Message, severity='CRITICAL')

def get_logger(mode, project_name, log_name):
    global logger
    if mode == "local":
        # Create and configure logger
        logging.basicConfig(filename="logfile.log",
                            format='%(asctime)s %(Message)s',
                            filemode='w')
        # Creating an object
        logger = logging.getLogger(log_name)
        # Setting the threshold of logger to DEBUG
        logger.setLevel(logging.DEBUG)
    else:
        logger = LoggingHandler(project_name, log_name, None)
    return logger

global logger
logger = get_logger(mode='cloud', project_name=PROJECT_ID, log_name='event_pipeline')


def create_dataset_if_not_exists():
    client = bigquery.Client()
    try:
        client.get_dataset(DATASET_ID)  # Make an API request.
        print('Dataset {} already exists'.format(DATASET_ID))
    except NotFound:
        print('Dataset {} is not found'.format(DATASET_ID))
        dataset_id = '{}.{}'.format(PROJECT_ID, DATASET_ID)
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = 'US'
        dataset = client.create_dataset(dataset)  # Make an API request.
        print('Created dataset {}.{}'.format(client.project,dataset.dataset_id))
    else:
        logger.error("Error in dataset creation")

def if_tbl_exists(client, table_ref):
    from google.cloud.exceptions import NotFound
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False
    else:
        print("Error in table creation")
        logger.error("Error in table creation")
    
# initialize Connector object
connector = Connector()

# function to return the database connection
def getconn() -> pymysql.connections.Connection:
    try:
        conn: pymysql.connections.Connection = connector.connect(
            "dev-world-terrorism-analytics:{rdb_region}:terrorism-instance",
            "pymysql",
            user= rdb_user,
            password=rdb_pwd,
            db=rdb_db
        )
    except Exception as e:
        print("Error in MySQL connection at", e)
        logger.error("Error in MySQL connection at", e)
    return conn

def pipeline():
    try:
        # create connection pool
        pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=getconn,
        )
        #extracting data from the database
        sql = "select * from event"
        event_df = pd.read_sql(sql, pool)
        sql = "select * from target"
        target_df = pd.read_sql(sql, pool)
        sql = "select * from weapon"
        weapon_df = pd.read_sql(sql, pool)
        sql = "select * from fatality"
        fatality_df = pd.read_sql(sql, pool)
        sql = "select * from `group`"
        group_df = pd.read_sql(sql, pool)
        sql = "select * from region"
        region_df = pd.read_sql(sql, pool)
        sql = "select * from attack"
        attack_df = pd.read_sql(sql, pool)
        sql = "select * from country"
        country_df = pd.read_sql(sql, pool)

        #merging all the tables to create the event fact table
        target_merge = pd.merge(target_df, country_df, on=["countryid"])
        target_merge = pd.merge(target_merge, region_df, on=["regionid"])
        target_merge = target_merge[['targetid','countryid','regionid']]
        event_merge = pd.merge(event_df, fatality_df, on = ["fatalityid"])
        event_fact = pd.merge(event_merge, target_merge, on = ["targetid"])

        #datapreprocessing
        event_fact = event_fact.replace(r'^\s*$', np.nan, regex=True)
        event_fact['latitude'] = event_fact['latitude'].fillna(method='ffill')
        event_fact['longitude'] = event_fact['longitude'].fillna(method='ffill')
        event_fact['crit1'] = event_fact['crit1'].replace(1,'Political and Economical')
        event_fact['crit1'] = event_fact['crit1'].replace(0, np.nan)
        event_fact['crit2'] = event_fact['crit2'].replace(1, 'Religious and Social')
        event_fact['crit2'] = event_fact['crit2'].replace(0, np.nan)
        event_fact['crit3'] = event_fact['crit3'].replace('1', 'Public Attack')
        event_fact['crit3'] = event_fact['crit3'].replace('0', np.nan)
        event_fact['date'] = event_fact['year'].astype(str)+'-'+event_fact['month'].astype(int).astype(str)+'-'+event_fact['day'].astype(int).astype(str)
        event_fact['date'] = pd.to_datetime(event_fact['date'], utc = True,infer_datetime_format=False,errors='coerce').dt.date
        event_fact['date'] = event_fact['date'].fillna(method='bfill')
        event_fact = event_fact.drop(columns=['year','month','day'])
        event_fact['propextent_txt'] = event_fact['propextent_txt'].fillna('Minor (likely < $1 million)')
        event =event_fact[['eventid','date','latitude','longitude','crit1','crit2','crit3','dbsource','success',
                            'nkill',
                            'nkillus',
                            'nkillter',
                            'nwound',
                            'nwoundte',
                            'property', 
                            'propextent',
                            'propextent_txt',
                            'propvalue',
                            'propcomment',
                            'ishostkid',
                            'nhostkid',
                            'nhostkidus',
                            'divert',
                            'kidhijcountry',
                            'ransom',
                            'ransomamt',
                            'ransomamtus',
                            'ransompaid',
                            'ransompaidus',
                            'ransomnote',
                            'hostkidoutcome',
                            'hostkidoutcome_txt',
                            'nreleased',
                            'targetid',
                            'gid',
                            'weaponid',
                            'attackid',
                            'countryid',
                            'regionid']]

        create_dataset_if_not_exists()

        table_id = '{}.{}'.format(DATASET_ID,TABLE_NAME)
        client = bigquery.Client(PROJECT_ID)
        dataset_name = client.dataset(DATASET_ID)
        table_ref = dataset_name.table(TABLE_NAME)
        table_exists_value = if_tbl_exists(client, table_ref)


        #Adding date column before creating table in bigquery
        event['UPDATED_ON'] = datetime.datetime.today()

        #create the table in bigquery
        if (table_exists_value == False):
            client.load_table_from_dataframe(event, table_id)
            print("Data has been loaded to a new table!")
            logger.info("Data has been loaded to a new table!")
        else:
            uploading_data_max_sdate = event.date.max()
            client = bigquery.Client()
            sql_query = """select max(date) as result from {}.{}.{}""".format(PROJECT_ID, DATASET_ID, TABLE_NAME) # find the max date from the bq table
            query_job = client.query(sql_query)
            results = query_job.result()
            existing_data_max_sdate = ''
            for row in results:
                existing_data_max_sdate = row.result
            print("existing", existing_data_max_sdate)
            print("uploading", uploading_data_max_sdate)
            if (uploading_data_max_sdate > pd.to_datetime(existing_data_max_sdate).date()):
                event = event[(event['date'] > pd.Timestamp(existing_data_max_sdate))]
                client.load_table_from_dataframe(event, table_id)
            print("Data has been loaded to a new table!")
            logger.info("Data has been loaded to a new table!")
        
    except Exception as e:
        print("Error in running the pipeline",e)
        logger.error("Error in running the pipeline")

pipeline()