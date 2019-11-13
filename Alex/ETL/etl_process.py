# import os
#pw = os.getenv('environment_variable_password')

# ETL Job


import time
import pymongo
import pandas as pd
import logging
logging.basicConfig(filename='debug.log', level=logging.ERROR)
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
#extract data from database

db = 'twitter'
table = 'tweets'
user = 'docker_user'
host = '0.0.0.0'
password = 'pw'
port = '5432'

input_conn = pymongo.MongoClient("0.0.0.0") #mongodb://admin:Password1@localhost:27017/test
output_conn = create_engine(f'postgres://{user}:{password}@{host}/{db}')
# client = MongoClient('mongodb://admin:Password1@localhost:27017/test')
# db = client.test
# restaurants = db.restaurants

def extract_data():
    json_data = input_conn.db.collections.find().tail(1)
    logging.info(str("New Tweet")) #logging
    return json_data

def data_transformer(json_data):
    df = pd.DataFrame(json_data)
    logging.info(str("Record Transformed")) #logging
    return df

def load_to_sql(df):
    df.to_sql('tweets', output_conn, if_exists = 'append', index = False)
    logging.info(str("Record posted to SQL")) #logging


extract_data >> transform_data >> load_df
