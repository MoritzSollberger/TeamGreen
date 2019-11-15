# ETL job
from sqlalchemy import create_engine
import pandas as pd
import os
import time
from pymongo import MongoClient
import pymongo
import logging
import sys
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# levels: DEBUG < INFO < WARNING < ERROR < CRITICAL
logging.basicConfig(filename='debug.log',
                    level=logging.INFO)  # only shows info and above logs

# 1. Extract data from DB 1

def extract_mongo(iteration):
    iteration = iteration
    client = MongoClient('mongodb')
    db = client['twitter_data']
    collection = db.tweets
    df = pd.DataFrame(list(collection.find({'sentimented':0})))
    #df = pd.DataFrame(list(collection.find().skip(5*iteration).limit(5)))
    
    for index in df.index:
        row = df.loc[index]
        db.tweets.update_one(
            {'_id': row['_id']},
            {'$set': {'sentimented': 1}},
            upsert=False)

    logging.debug(str(df.head(1)))
    return df

# 2. Transform it
def sentiment_analysis(df):
    analyzer = SentimentIntensityAnalyzer()
    polarity = []
    for tweet in df['text'].astype(str):
        polar = analyzer.polarity_scores(tweet)
        polarity.append(polar['compound'])
    return polarity

def transform_data(df):
    # df['length'] = df['name'].apply(len)
    #df['id'] = df['_id'].astype(str)
    del df['entities']
    df.set_index('id', inplace=True)
    df['sentiment'] = sentiment_analysis(df)

    del df['_id']
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
iteration = 0
while True:
    load_sql(transform_data(extract_mongo(iteration)))
    iteration += 1
    time.sleep(6)
