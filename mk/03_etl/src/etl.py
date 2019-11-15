
# ETL Job
from pymongo import MongoClient
from bson import json_util
from sqlalchemy import create_engine
import pandas as pd
import time
import pymongo
import logging
import os
import pprint
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# 5 levels: DEBUG < INFO < WARNING < ERROR < CRITICAL
# logging.basicConfig(filename='debug.log', level = logging.INFO) # filename = ’debug.log'

#%%

# 1. extract data from DB 1
HOST = 'localhost'
PORT = 27019
DB = 'twitter_data'
COLLECTION = 'tweets' #table

def extract_data():
    client = MongoClient(HOST, PORT)
    collection = client[DB][COLLECTION]
    df = pd.DataFrame(collection.find().limit(10))
    return df
    # texts = []
        # for tweet in collection.find().limit(10):
    #     texts.append(tweet['text'])
    # return df
    # return texts

data = extract_data()
data
#%%


def clean_data(df):
    """
    function used to create new column and clean (e.g. 're-tweet' - information,
    special characters) the 'text' collected from the API.
    """
    cleaned = []
    for row,i in zip(df['text'],df.index):
        if ':' in row:
            row = row.split(':')[1]
        a = re.sub('[\.@]', '', row)
        cleaned.append(a)
    df['clean_text'] = pd.Series(cleaned)

    return df

data1 = clean_data(data)
data1['text'][0]
data1['clean_text'][0]
#%%
# sentiment analysis

analyzer = SentimentIntensityAnalyzer()

for text in clean_texts:
    print(analyzer.polarity_scores(text))

#%%
user = 'postgres'
db = 'twitter '


def load_sql(df):
    engine = create_engine(f'postgres://{user}:{user}@{host}:{port}/{db}')
    df = df.to_sql(table, engine, if_exists='append')
    logging.debug(str(df))  # logging
    return df
    #print(df.shape)







# 2. transform it
def transform_data(df):
    df['length'] = df['user'].apply(len)
    df.set_index('name', inplace=True)
    logging.debug(str(df))
    print(df)
    return df

#%%
# 3. load it to DB2

def load_data(df):
    json = df.to_dict()
    json['timestamp'] = time.asctime()
    print(json)
    mongo_conn = pymongo.MongoClient("0.0.0.0:27017")
    mongo_conn.db.collections.names.insert(json)
    # to start up mongo in a docker container:
    # docker run -it -d -p 27017:27017 mongo

#%%
# call the functions

df = extract_data()
df = transform_data(df)
load_data(df)

# functional style
# load_data(transform_data(extract_data()))

# in Airflow this would look like this:
# extract_data >> transform_data >> load_data
