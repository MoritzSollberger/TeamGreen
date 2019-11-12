from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import json
import pandas as pd

from credentials import cfg

def authenticate():
    """Function used for handling twitter authentication"""
    auth = OAuthHandler(cfg['CONSUMER_API_KEY'], cfg['CONSUMER_API_SECRET'])
    auth.set_access_token(cfg['ACCESS_TOKEN'], cfg['ACCESS_TOKEN_SECRET'])
    return auth

def write_tweet(tweet_dict):

    df = pd.DataFrame(index = [0, 1], data=tweet_dict)
    df.to_csv('test.csv', mode='a')

class TwitterStreamer(StreamListener):

    def on_data(self, data):

        """Whatever we put in this method defines what is done with every
        single tweet as it is intercepted in real-time"""
        tweet = json.loads(data)

        tweet_dict = {'user': tweet['user']['screen_name'],
                      'text': tweet['text']}
        print(tweet_dict)
        write_tweet(tweet_dict)



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

    stream.filter(track=['berlin'], languages=['en'])
