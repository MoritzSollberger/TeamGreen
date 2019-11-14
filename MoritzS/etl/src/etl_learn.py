# ETL job
from sqlalchemy import create_engine
import pandas as pd
import os
import time
from pymongo import MongoClient
import pymongo
import logging
import sys

# levels: DEBUG < INFO < WARNING < ERROR < CRITICAL
logging.basicConfig(filename='debug.log',
                    level=logging.INFO)  # only shows info and above logs

# 1. Extract data from DB 1

def extract_mongo():
    client = MongoClient('mongodb')
    db = client['twitter_data']
    collection = db.tweets
    df = pd.DataFrame(list(collection.find().limit(5)))
    logging.debug(str(df.head(1)))
    return df

# 2. Transform it
def transform_data(df):
    # df['length'] = df['name'].apply(len)
    df['id'] = df['_id'].astype(str)
    del df['_id']
    del df['original tweet']
    df.set_index('id', inplace=True)
    logging.debug(str(df))  # logging
    return df


# 3. Load it into DB 2
db = 'twitter'
table = 'tweets'
user = 'postgres'
host = 'postgres'
port = '5432'

def load_sql(df):
    engine = create_engine(f'postgres://{user}:{user}@{host}:{port}/{db}')
    df = df.to_sql(table, engine, if_exists='append')
    logging.debug(str(df))  # logging
    return df
    #print(df.shape)

# functional programming
while True:
    load_sql(transform_data(extract_mongo()))
    time.sleep(6)
