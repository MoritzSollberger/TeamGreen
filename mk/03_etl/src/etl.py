
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

#%%

# 1. extract data from DB 1
HOST = 'localhost'
PORT = 27019
DB = 'twitter_data'
COLLECTION = 'tweets' #table

def extract_data():
    """
    function that pulls the tweets from the mongodb into DataFrame
    """
    client = MongoClient(HOST, PORT)
    collection = client[DB][COLLECTION]
    df = pd.DataFrame(collection.find().limit(10))
    return df

#%%
# clean the data (for now just kick out links)
def clean_data(df):
    """
    function used to create new column and clean (e.g. 're-tweet' - information,
    special characters) the 'text' collected from the API.
    """
    cleaned = []
    for row,i in zip(df['text'],df.index):
        # if ':' in row:
        #     row = row.split(':')[1]
        text = re.sub('https:[\w.\/]*','',row)
        # a = re.sub(r'[\.@]', '', row)
        cleaned.append(text)
    df['clean_text'] = pd.Series(cleaned)
    return df

#%%
# transform the data (-> sentiment anlysis)
def sentiment_analysis(df):
    """
    function used to determine the sentiment of the tweets
    """
    analyzer = SentimentIntensityAnalyzer()
    polarity = []
    for tweet in df['clean_text'].astype(str):
        sentiment = analyzer.polarity_scores(tweet)
        polarity.append(sentiment['compound'])
    df['sentiment'] = pd.Series(polarity)
    return df
#%%
# call the function: functional style

data_final = sentiment_analysis(clean_data(extract_data()))
data_final

#%%
# DataFrame to SQL-table
db = 'twitter'
table = 'tweets'
user = 'postgres'
password = 'postgres'
host = '04_sql'
port = '5432'

def load_sql(df):
    """
    pushes the DataFrame to the postgres container into a table
    in the twitter database
    """
    engine = create_engine(f'postgres://{user}:{user}@{host}:{port}/{db}')
    df = df.to_sql(table, engine, if_exists='append')
    logging.debug(str(df))  # logging
    # return df
    #print(df.shape)

#%%
# call all function together

load_sql(data_final)
