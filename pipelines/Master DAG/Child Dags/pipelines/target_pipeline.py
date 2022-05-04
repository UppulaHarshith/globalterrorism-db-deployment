import os
os.system("pip3 install pymysql")
os.system("pip3 install sqlalchemy")
os.system("pip3 install sqlalchemy_utils")
os.system("pip3 install cloud-sql-python-connector[pymysql]")
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
TABLE_NAME = 'target_dwh'
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
logger = get_logger(mode='cloud', project_name=PROJECT_ID, log_name='target_pipeline')

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
        # initialize Connector object
        connector = Connector()
        # create connection pool
        pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=getconn,
        )
        #extracting data from the database
        sql = "select * from target"
        target_df= pd.read_sql(sql, pool)



        # datapreprocessing
        target_df=target_df.replace(r'^\s*$', np.nan, regex=True)
        target_df['targetype'] = target_df[['targtype1', 'targtype2', 'targtype3']].bfill(axis=1).iloc[:, 0]
        target_df['target'] = target_df[['target1', 'target2', 'target3']].bfill(axis=1).iloc[:, 0]
        target_df['targetsubtype'] = target_df[['targsubtype1', 'targsubtype2', 'targsubtype3']].bfill(axis=1).iloc[:, 0]
        target_df['targetsubtype_txt'] = target_df[['targsubtype1_txt', 'targsubtype2_txt', 'targsubtype3_txt']].bfill(axis=1).iloc[:, 0]
        target_df['targetype_txt'] = target_df[['targtype1_txt', 'targtype2_txt', 'targtype3_txt']].bfill(axis=1).iloc[:, 0]
        target_df['corp'] = target_df[['corp1', 'corp2', 'corp3']].bfill(axis=1).iloc[:, 0]
        target_df['nationality'] = target_df[['natlty1', 'natlty2', 'natlty3']].bfill(axis=1).iloc[:, 0]
        target_df['nationality_text'] = target_df[['natlty1_txt', 'natlty2_txt', 'natlty3_txt']].bfill(axis=1).iloc[:, 0]
        target_df= target_df[[ 'targetid','targetype', 'targetype_txt','targetsubtype','targetsubtype_txt','target','corp','nationality','nationality_text']]
        target_df.fillna('Unknown')
        target_df=target_df.replace('?','')
        target_df = target_df.drop_duplicates()
        create_dataset_if_not_exists()

        table_id = '{}.{}'.format(DATASET_ID, TABLE_NAME)
        client = bigquery.Client(PROJECT_ID)
        dataset_name = client.dataset(DATASET_ID)
        table_ref = dataset_name.table(TABLE_NAME)
        table_exists_value = if_tbl_exists(client, table_ref)

        #Adding date column before creating table in bigquery
        target_df['UPDATED_ON'] = datetime.datetime.today()

        # create the table in bigquery
        if (table_exists_value == False):
            client.load_table_from_dataframe(target_df, table_id)
            print("Data has been loaded to a new table!")
            logger.info("Data has been loaded to a new table!")
        else:
            client = bigquery.Client()
            sql_query = """select targetid from {}.{}.{}""".format(PROJECT_ID, DATASET_ID, TABLE_NAME) # find the max date from the bq table
            bq_df = client.query(sql_query).to_dataframe()
            target_merge = target_df.merge(bq_df.drop_duplicates(), on=['targetid'], 
                   how='left', indicator=True)
            target_merge = target_merge[target_merge['_merge'] == 'left_only']
            client.load_table_from_dataframe(target_merge, table_id)
            print("Data has been appended to existing table!")
            logger.info("Data has been appended to existing table!")

    except Exception as e:
        print("Error in running the pipeline", e)
        logger.error("Error in running the pipeline")

pipeline()