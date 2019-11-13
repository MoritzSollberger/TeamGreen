# ETL job
from sqlalchemy import create_engine
import pandas as pd
import os
import time
import pymongo
import logging
import sys

# levels: DEBUG < INFO < WARNING < ERROR < CRITICAL
logging.basicConfig(filename='debug.log',
                    level=logging.INFO)  # only shows info and above logs

# 1. Extract data from DB 1
db = 'babynames'
table = 'babynames'
user = 'moritzsollberger'
host = '0.0.0.0'
#password = os.getenv('POSTGRES_PASSWORD')


def extract_data():
    engine = create_engine(f'postgres://{user}@{host}/{db}')
    df = pd.read_sql(f'SELECT * FROM {table} LIMIT (10)', engine)
    logging.debug(str(df))  # logging
    return df
    #print(df.shape)


# 2. Transform it
def transform_data(df):
    df['length'] = df['name'].apply(len)
    df.set_index('name', inplace=True)
    logging.debug(str(df))  # logging
    return df


# 3. Load it into DB 2
def load_data(df):
    json = df.to_dict()
    json['timestamp'] = time.asctime()
    logging.warning(str(json))  # logging

    mongo_connection = pymongo.MongoClient()
    # docker run -it -d -p 27017_27017 mongo
    collections = mongo_connection.db.collections.names_analytics.insert(json)
    print('done')
    return df


# functional programming
load_data(transform_data(extract_data()))
