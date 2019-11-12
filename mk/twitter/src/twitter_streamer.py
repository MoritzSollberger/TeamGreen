from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import json
import sys
import pandas as pd
import pymongo

HOST = 'localhost'
PORT = '27017'
CLIENT = pymongo.MongoClient(f'mongodb://{HOST}:{PORT}')
DB = CLIENT.twitter_data

sys.path.append('/Users/MK/python/team_red/mk')
from tweepy_script import cfg

#%%

def authenticate():
    """
    Function used for handling twitter authentication
    """
    auth = OAuthHandler(cfg['consumer_key'], cfg['consumer_secret'])
    auth.set_access_token(cfg['access_token'], cfg['access_token_secret'])
    return auth

def write_tweet(tweet_dict):
    """
    function used to load the scraped tweet into a pandas DataFrame and
    put it into a csv on the computer
    """
    df = pd.DataFrame(index=[0,1], data=tweet_dict)
    df.to_csv('test.csv', mode='a')

def load_into_mongo(t):
    """
    function loads the collected tweet into a mongo database
    """
    # twitter_data (DB) --> tweets (collection) --> tweet_dict (documents)
    DB.tweets.insert(t)
    print(f"the verrückte Mongo is {t['user']}!")


#%%
class TwitterStreamer(StreamListener):


    def on_data(self, data):
        """
        Whatever we put in this method defines what is done with every single
        tweet as it is intercepted in real-time
        """
        tweet = json.loads(data)
        # print(tweet['text'])
        # print(tweet['user']['screen_name'])

        tweet_dict = { 'user': tweet['user']['screen_name'],
                        'text': tweet['text'],
                        'original_tweet': tweet}
        # # print(tweet_dict)
        # write_tweet(tweet_dict)
        load_into_mongo(tweet_dict)


    def on_error(self, status):

        if status == 420:
            """
            If rate-limiting occurs
            """
            print(status)
            return False

#%%

if __name__ == '__main__':
    """
    the following code should be run ONLY if I type
    'python twitter_streamer.py' in the terminal
    """
    # 1. authenticate ourselves
    auth = authenticate()

    # 2. instantiate out Twitter Streamer
    streamer =  TwitterStreamer()

    # 3. wrap the 2 variables into a Stream object to
    # actually start the stream
    stream = Stream(auth, streamer)

    stream.filter(track=['berlin'], languages=['en'])
