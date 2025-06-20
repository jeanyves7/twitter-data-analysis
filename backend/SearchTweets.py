import pandas as pd, subprocess, sys, datetime, tweepy as tw, os
import logging
from configparser import ConfigParser
from sqlalchemy import create_engine
from .DatabaseConnection import clean, connect
from .app import conn
from .Rds_Handle import (
    get_waiting_query,
    close_engine_search,
    update_previous_study,
)
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="twitter_analysis")

# # URL to connect to redshift

URL = os.getenv('Redshift_URL')

# getting the engine for the redshift ready
engine = create_engine(URL)

logger = logging.getLogger(__name__)


def insert_panda(lp, name):
    try:
        name = clean([' ', '-'], name, '_')
        data = pd.DataFrame(lp)
        data.to_sql(name, engine, index=False, if_exists='append')
    except Exception as e:
        logger.error("An error occurred while inserting the data " + e.__str__())


def create_headers(engine_name, filename='credentials.ini'):
    try:
        

        consumer_key = os.getenv('{}_c_key'.format(engine_name))
        consumer_secret = os.getenv('{}_c_secret'.format(engine_name))
        access_key = os.getenv('{}_a_key'.format(engine_name))
        access_secret = os.getenv('{}_a_secret'.format(engine_name))

        return consumer_key, consumer_secret, access_key, access_secret
    except Exception as e:
        logger.error("Error encountered : " + e.__str__())



def handle_tweet(tweet, country=""):
    try:
        Ftext = tweet["full_text"]
        hashes = ""
        if tweet['coordinates']:
            data = tweet['coordinates']['coordinates']
            country = "{},{}".format(str(data[0]), str(data[1]))
        for hashe in tweet["entities"]["hashtags"]:
            if hashe is not None:
                hashes += hashe["text"] + " "
        logger.debug("hashes: %s", hashes)
        logger.debug(tweet['favorite_count'] + tweet['retweet_count'])
        logger.debug(tweet['user']['description'])
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
            Ftext = tweet["retweeted_status"]["full_text"]
        else:
            dicti['lang'] = tweet['user']['lang']

        # cleaning the data before adding it to the database by removing the chars that may raise an error
        p = clean([",", "\n"], Ftext, ' ')

        dicti['text'] = p
        return dicti
    except Exception as e:
        logger.error("Error occurred in Handling tweet {}".format(e.__str__()))
    #  return False


def check_time(stop_date):
    actual_date = datetime.datetime.now()
    logger.debug("actual date: %s VS stop_date: %s", actual_date, stop_date)

    if actual_date >= stop_date:
        return True
    return False


def get_tweepy_stream(tweepy_query: str, stop_date, country=""):
    logger.info(
        "starting getting data from tweepy from %s at %s",
        country,
        datetime.datetime.now().minute,
    )
    search = tweepy_query + " #{}".format(tweepy_query.replace(" ", "_"))

    tweets = tw.Cursor(api.search,
                       q=search,
                       tweet_mode="extended", lang="en", count=100, max_id='1383825632480559109').items()

    c = 1
    list_tweets = []
    try:
        # mapping through the items from the tweet call
        for tweet in tweets:
            # print(tweet._json)
            temp = handle_tweet(tweet._json, country)
            # translate
            logger.debug("count: %s %s", c, temp)
            list_tweets.append(temp)
            c += 1
            # for every 150 tweet found we will insert them in the database
            if c == 150:
                logger.info("inserting in the database")
                insert_panda(list_tweets, tweepy_query)
                c = 0
                list_tweets = []
                if check_time(stop_date):
                     break

    except Exception as e:
        logger.error("Error occurred {}".format(e.__str__()))


if __name__ == "__main__":
    try:
        q = sys.argv
        query = q[1]
        to = q[2]
        # incrementing the stop time
        time = int(q[3])
        per = q[4]
        date = datetime.datetime.now()
        #date += datetime.timedelta(minutes=time)
        date += datetime.timedelta(days=time)
        logger.debug(f"{date.day}  {date.month}  {date.minute}  {date.hour}")
        # creating the table if not exists
        connect(query)
        # getting the credentials
        headers = create_headers(per)
        auth = tw.OAuthHandler(headers[0], headers[1])
        auth.set_access_token(headers[2], headers[3])
        api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        # Opening the liveStreamApi  algorithm
        subprocess.Popen(
            'python backend/streamApi.py {0} {1} {2}'.format(query, time, per),
            shell=True,
        )
        get_tweepy_stream(query, date)
        close_engine_search(query, conn)
        update_previous_study(study=query, report=False, start=False, conn=conn)
        get_waiting_query(conn)
        # calling the data analysis algorithm
        query = clean([' ', '-'], query, '_')
        subprocess.Popen(
            'python backend/SentimentAnalysis.py {}'.format(query), shell=True
        )
        logger.info("done")
    except Exception as e:
        logger.error("Error occurred in searchTweet {}".format(e.__str__()))
