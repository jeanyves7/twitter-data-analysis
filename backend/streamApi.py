import datetime
import os
import tweepy
import pandas as pd
from typing import Optional
import logging
from sqlalchemy import create_engine
from tweepy import Stream, StreamListener
from .SearchTweets import check_time
from geopy.geocoders import Nominatim
from queue import Queue
from threading import Thread

# Database connection URL (PostgreSQL / Redshift)
URL = os.getenv('POSTGRES_URL') or os.getenv('Redshift_URL') or os.getenv('REDSHIFT_URL')

# create SQLAlchemy engine
engine = create_engine(URL)
logger = logging.getLogger(__name__)
# authorization tokens will be created in start_stream
auth = None
api: Optional[tweepy.API] = None


def clean(liste, name, rep):
    for i in liste:
        name = name.replace(i, rep)
    return name


def insert_panda(lp, name):
    try:
        name = clean([' ', '-'], name, '_')
        data = pd.DataFrame(lp)
        data.to_sql(name, engine, index=False, if_exists='append')
    except Exception as e:
        logger.error("An error occurred while inserting the data " + e.__str__())


def handle_tweet(tweet, ex):
    try:
        status = tweet
        tweet = tweet._json
        if ex:
            Ftext = status.extended_tweet["full_text"]
        else:
            Ftext = tweet["text"]
        country = ""
        if tweet['coordinates']:
            data = tweet['coordinates']['coordinates']
            # location = geolocator.reverse("{}, {}".format(str(data[1]), str(data[0])))
            # country = location.raw['address']['country']
            country = "{},{}".format(str(data[0]), str(data[1]))
        hashes = ""
        for hashe in tweet["entities"]["hashtags"]:
            if hashe is not None:
                hashes += hashe["text"] + " "
        des = str(tweet['user']['description'])
        description = clean([",", "\n"], des, ' ')
        dicti = {'id': tweet['id_str'], 'name': tweet["user"]["name"], 'screen_name': tweet["user"]["screen_name"],
                 'retweets': tweet['retweet_count'], 'likes': tweet['favorite_count'], 'description': description,
                 'verified': str(tweet['user']['verified']), 'geo': country,
                 "hashes": hashes,
                 'followers': str(tweet['user']['followers_count']),
                 'followingC': str(tweet['user']['friends_count'])}
        if "retweeted_status" in tweet.keys():
            dicti['lang'] = tweet['retweeted_status']['user']['lang']
            if ex:
                Ftext = tweet["user"]["extended_tweet"]["full_text"]
            else:
                Ftext = tweet["retweeted_status"]["text"]

        else:
            dicti['lang'] = tweet['user']['lang']

        # cleaning the data before adding it to the database by removing the chars that may raise an error
        p = clean([",", "\n"], Ftext, ' ')
        dicti['text'] = p
        return dicti
    except Exception as e:
        logger.error("Error occurred in handling tweet {}".format(e))


class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """

    def __init__(self, q=Queue()):
        super().__init__()
        self.q = q
        self.tweet_count = 1
        self.liste_tweets = []
        self.query = ""
        self.stop_date = ""

        t = Thread(target=self.do_stuff)
        t.daemon = True
        t.start()

    def on_status(self, status):

        extended = False
        if hasattr(status, "extended_tweet"):
            extended = True
        tweet = handle_tweet(status, extended)

        self.tweet_count += 1
        self.liste_tweets.append(tweet)

    def do_stuff(self):
        while True:
            if self.tweet_count % 500 == 0:
                for i in self.liste_tweets:
                    logger.debug(i)
                logger.info("adding to the database")
                insert_panda(self.liste_tweets, self.query)
                logger.info("done inserting")
                break
            

    def on_error(self, status):
        logger.error(status)


l: StdOutListener = StdOutListener()


def main(query, stop):
    global l
    l.query = query
    l.stop_date = stop
    try:
        stream = Stream(auth=auth, listener=l, tweet_mode='extended')
        stream.filter(track=query, languages=['en'])
    except Exception as e:
        logger.error("Error occurred in stream api : {}".format(e.__str__()))


def get_active_stream():
    return l.liste_tweets


def start_stream(query, time, per):
    global auth, api
    consumer_key = os.getenv(f"{per}_c_key")
    consumer_secret = os.getenv(f"{per}_c_secret")
    access_key = os.getenv(f"{per}_a_key")
    access_secret = os.getenv(f"{per}_a_secret")

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    logger.info("we are in the Live stream")
    date = datetime.datetime.now()
    date += datetime.timedelta(minutes=time)
    main(query, date)
    logger.info("done")


if __name__ == '__main__':
    q = sys.argv
    start_stream(q[1], int(q[2]), q[3])
