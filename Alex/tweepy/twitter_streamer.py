from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import json
import pandas as pd
import pymongo
import re
from credentials import cfg

CLIENT = pymongo.MongoClient('mymongo')
DB = CLIENT.twitter_data

KEYWORDS = ['Berlin']


def authenticate():
    """Function used for handling twitter authentication"""
    auth = OAuthHandler(cfg['CONSUMER_API_KEY'], cfg['CONSUMER_API_SECRET'])
    auth.set_access_token(cfg['ACCESS_TOKEN'], cfg['ACCESS_TOKEN_SECRET'])
    return auth

def load_to_mongo(tweet):
    """
    twitter_data (DB) -> tweets (collection) -> tweet -> (document)
    """
    DB.tweets.insert(t)

def clean_text(t):

    text = re.sub('https:\/\/[\w.\/]*','',t['text'])

    hashtags = []
    if 'extended_tweet' in t['original_tweet']:
        if 'hashtags' in t['original_tweet']['extended_tweet']['entities']
            for hashtag in t['original_tweet']['extended_tweet']['entities']['hashtags']:
            hashtags.append(hashtag['text'])
    else:
        hashtags = []

    return text, hashtags


class TwitterStreamer(StreamListener):

    def on_data(self, data):
        """
        Whatever we put in this method defines what is done with every
        single tweet as it is intercepted in real-time
        """
        tweet = json.loads(data)
        text, hastags = clean_text(tweet)

        tweet_dict =    {'created_at': tweet['created_at'],
                 'id': tweet['id_str'],
                 'text': text,
                 'username': tweet['user']['screen_name'],
                 'followers':tweet['user']['followers_count'],
                 'user_favorites_count': tweet['user']['favourites_count'],
                 'retweets': tweet['retweet_count'],
                 'favorites': tweet['favorite_count'],
                 'hashtags': hashtags,
                 'interesting': 0}

        # print(tweet_dict)
        # write_tweet(tweet_dict)
        load_to_mongo(tweet_dict)


    def on_error(self, status):
        """If rate-limiting occurs"""
        if status == 420:
            print(status)
            return False

if __name__ == '__main__':

    """the following code should be run ONLY when I type
       'python twitter_streamer.py' in the terminal """
    # 1. Authenticate ourselves
    auth = authenticate()

    # 2. Instantiate our Twiter Streamer
    streamer = TwitterStreamer()

    #3. Wrap the 2 variables into a Stream object to actually
    # start the stream
    stream = Stream(auth, streamer)

    stream.filter(track=KEYWORDS, languages=['en'])
