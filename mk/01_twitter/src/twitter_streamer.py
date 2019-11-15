from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener

# in a separate file the credentials
from config import cfg
import json
import pandas as pd
import pymongo
import time

HOST = 'localhost'
PORT = '27017'
#CLIENT = pymongo.MongoClient(f'mongodb://{HOST}:{PORT}')
CLIENT = pymongo.MongoClient('mongodb')
DB = CLIENT.twitter_data

# counter = 0

def authenticate():
    """
    Function used to handle Twitter authentication
    """
    auth = OAuthHandler(cfg['CONSUMER_API_KEY'], cfg['CONSUMER_API_SECRET'])
    auth.set_access_token(cfg['ACCESS_TOKEN'], cfg['ACCESS_TOKEN_SECRET'])
    return auth


def write_tweet(tweet_dict):
    df_old = pd.read_csv('test.csv')
    df = pd.DataFrame(tweet_dict, index=[0, 1])
    df.to_csv('test.csv')


def load_into_mongo(tweet_dict):
    """
    twitter_data (DB) ---> tweets (collection) ---> tweet_dict (documents)
    """
    DB.tweets.insert(tweet_dict)
    print(f"the verückte Mongo is: {tweet_dict['user']}")


class TwitterStreamer(StreamListener):

    def on_data(self, data):
        """
        Defines what happens with every single tweet as it is intercepted in
        real time
        """
        tweet = json.loads(data)

        tweet_dict = {
            'user': tweet['user']['screen_name'],
            'text': tweet['text'],
            'date': tweet['created_at'],
            'location': tweet['user']['location'],
            'followers': tweet['user']['followers_count'],
            'friends': tweet['user']['friends_count'],
            'user_since': tweet['user']['created_at'],
            'original tweet': tweet
        }

        # write_tweet(tweet_dict)
        load_into_mongo(tweet_dict)
        # counter +=1
        # if counter = 10:
        #     return False

    def on_error(self, status):

        if status == 420:

            print(status)
            return False


# if executed directly, call this. Otherwise no.
if __name__ == '__main__':
    """
    The following code should be run only when i type
    python twitter_streamer.py
    in the terminal
    """

    # 1. Authenticate outselfes
    auth = authenticate()

    # 2. Instantiate out Twitter StreamListener
    streamer = TwitterStreamer()

    # 3. Wrap the 2 variables into a Stream object to actually
    # start the stream
    stream = Stream(auth, streamer)

    stream.filter(track=['#climate', '#ClimateChange', '#ClimateCrisis', '#ClimateEmergency'], languages=['en'])
