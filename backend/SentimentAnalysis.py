import subprocess
import logging

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import re
import os
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import spacy, sys
from .Rds_Handle import (
    get_date,
    create_analyse_table,
    insert_analysed_data,
    update_previous_study,
    get_analysed_study,
)
from .app import conn
from collections import Counter
# import enchant
from english_words import english_words_set

q = sys.argv

# available tables: pizza, covid, popcorn, vodka, dollar, quarantine, burger

engine = create_engine(os.getenv("REDSHIFT_URL"))
logger = logging.getLogger(__name__)

study = q[1]
query = "SELECT * FROM {}".format(study)

df = pd.read_sql(sql=query, con=engine)
# Saves the number of tweets in a variable
number_of_tweets = len(df.index)
# df.drop_duplicates(subset="text", keep=False, inplace=True)

sentimentAnalyzer = SentimentIntensityAnalyzer()
nlp = spacy.load('en_core_web_md')


# Function that removes regex pattern
def remove_pattern(text, pattern):
    r = re.findall(pattern, text)
    for i in r:
        text = re.sub(i, ' ', text)
    return text


# Removes @, URLS, and special characters
def clean_tweets(tweets):
    searchPatterns = [r"@\S*", r"https?:\/\/\S*"]
    removePatterns = [r"@\S*", r"https?:\S*"]
    metacharacters = "()[]{}^*+$.\|?/"
    specialCharacters = "#$%&*+\",-./;<=>?@[\\]^_{|}~`"
    for pattern in searchPatterns:
        wordsToRemove = re.findall(pattern, tweets)
        for element in wordsToRemove:
            newElement = element
            for metacharacter in metacharacters:
                newElement = newElement.replace(metacharacter, '')
            tweets = tweets.replace(element, newElement)
    for pattern in removePatterns:
        tweets = np.vectorize(remove_pattern)(tweets, pattern)
        tweets = np.array_str(tweets)
    for specialCharacter in specialCharacters:
        tweets = tweets.replace(specialCharacter, ' ')
    tweets = (" ".join(tweets.split()))
    tweets = tweets.replace('\n', ' ')
    if tweets == '':
        return tweets
    if tweets[0] == ' ':
        tweets = tweets[1:]
    return tweets


# Lemmatize words and remove stopwords for worldcloud
def preprocess_tweets(tweets):
    wordsToRemove = re.findall(r'[^A-Za-z]', tweets)
    for element in wordsToRemove:
        newElement = element
        metacharacters = "()[]{}^*+$.\|?/"
        for metacharacter in metacharacters:
            newElement = newElement.replace(metacharacter, '')
        tweets = tweets.replace(element, newElement)
    tweets = np.vectorize(remove_pattern)(tweets, r'[^A-Za-z]')
    tweets = np.array_str(tweets)
    specialCharacters = "#$%&*+\",-./;<=>?@[\\]^_{|}~`:\'()!?"
    for specialCharacter in specialCharacters:
        tweets = tweets.replace(specialCharacter, ' ')
    doc = nlp(tweets)
    tweets = ''
    lemma_list = []
    filtered_list = []
    for token in doc:
        lemma_list.append(token.lemma_)
    for word in lemma_list:
        token = nlp.vocab[word]
        if not token.is_stop:
            filtered_list.append(word)
    for word in filtered_list:
        tweets = tweets + ' ' + ''.join(str(word))
    tweets = (" ".join(tweets.split()))
    tweets = tweets.lower()
    return tweets


# Returns the sentiment and the compound score of each tweet
# Saves the total number of positive, neutral, negative and the compound score of all tweets
positive_tweets = 0
neutral_tweets = 0
negative_tweets = 0
all_compound_scores = 0


def sentiment_scores(tweets):
    global positive_tweets
    global neutral_tweets
    global negative_tweets
    global all_compound_scores
    sentiment_list = []

    sentiments = sentimentAnalyzer.polarity_scores(tweets)
    sentiment_list.append(sentiments['compound'])
    all_compound_scores = all_compound_scores + sentiment_list[0]

    if sentiment_list[0] >= 0.05:
        positive_tweets += 1
        sentiment_list.append('Positive')
    elif sentiment_list[0] <= - 0.05:
        negative_tweets += 1
        sentiment_list.append('Negative')
    elif 0.05 >= sentiment_list[0] >= - 0.05:
        neutral_tweets += 1
        sentiment_list.append('Neutral')
    return pd.Series((sentiment_list[0], sentiment_list[1]))


# Gets the overall compound score of all tweets
# You have to run get_compound_scores first to get the compound scores of all the tweets
overall_compound_score = 0


def get_overall_score():
    global overall_compound_score
    overall_compound_score = all_compound_scores / number_of_tweets
    return overall_compound_score


# Gets the overall sentiment of all tweets on the subject
def get_overall_sentiment():
    global overall_compound_score
    if overall_compound_score >= 0:
        return 'Positive'
    else:
        return 'Negative'


def wordcloud_list(tweets):
    word_freq = Counter()
    for tweet in tweets:
        tweet = tweet.split()
        for word in tweet:
            if len(word) > 3 and (word in english_words_set) and word != study:
                word_freq.update(word.split())
    return word_freq.most_common(18)


# Get the percentage of each type of users
percentage_of_other_users = 0
percentage_of_companies = 0
percentage_of_influencers = 0
total_nb_of_users = 0


def user_type_percentages(usernames):
    global percentage_of_other_users
    global percentage_of_companies
    global percentage_of_influencers
    global total_nb_of_users
    nb_of_other_users = 0
    nb_of_companies = 0
    nb_of_influencers = 0

    for user in usernames:
        if user == 'X':
            nb_of_other_users += 1
        else:
            doc = nlp(user)
            for entity in doc.ents:
                if entity.label_ == 'ORG':
                    nb_of_companies += 1
                elif entity.label_ == 'PERSON':
                    nb_of_influencers += 1
                else:
                    nb_of_other_users += 1
    total_nb_of_users = nb_of_companies + nb_of_other_users + nb_of_influencers
    percentage_of_companies = nb_of_companies
    percentage_of_influencers = nb_of_influencers
    percentage_of_other_users = nb_of_other_users


# Get most involved companies
def word_cloud_companies(usernames):
    company_freq = Counter()
    for user in usernames:
        doc = nlp(user)
        for entity in doc.ents:
            if entity.label_ == 'ORG':
                company_freq.update({entity.text: 1})
    return company_freq.most_common(10)


# Get set of companies that have tweeted on the subject
def get_set_of_companies(usernames):
    set_of_companies = set()
    for user in usernames:
        doc = nlp(user)
        for entity in doc.ents:
            if entity.label_ == 'ORG':
                set_of_companies.add(entity.text)
    return set_of_companies


def clean_hashtags(hashtags):
    specialCharacters = "#$%&*+\",-./;<=>?@[\\]^_{|}~`:\'()!?"
    for specialCharacter in specialCharacters:
        hashtags = hashtags.replace(specialCharacter, ' ')
    return hashtags


# Generate a wordcloud from most used hashtags
def hashtag_wordcloud(hashtags):
    hashtag_freq = Counter()
    for hashtag in hashtags:
        hashtag = hashtag.split()
        for word in hashtag:
            if len(word) > 3 and (word in english_words_set) and word != study:
                hashtag_freq.update(word.split())
    return hashtag_freq.most_common(18)


# Store top countries of tweets origin
def geo_location_list(locations):
    location_freq = Counter()
    for location in locations:
        if location == '':
            location_freq.update({'Others': 1})
        doc = nlp(location)
        for entity in doc.ents:
            if entity.label_ == 'GPE':
                location_freq.update({entity.text: 1})
            else:
                location_freq.update({'Others': 1})
    return location_freq.most_common(18)


# New functions

def get_list_of_coordinates(coordinates):
    list_of_coordinates = []
    for coord in coordinates:
        if coord != '' and coord != 'None':
            coord = coord.split(',')
            list_of_coordinates.append(tuple(coord))
    return list_of_coordinates


def get_sentiment(tweet):
    sentiment = sentimentAnalyzer.polarity_scores(tweet)
    if sentiment['compound'] >= 0.05:
        return 'Positive'
    elif sentiment['compound'] <= - 0.05:
        return 'Negative'
    elif sentiment['compound'] and sentiment['compound'] >= - 0.05:
        return 'Neutral'


likes_number = 0


def get_most_liked(likes):
    global likes_number
    for like in likes:
        likes_number = likes_number + like
    return likes.idxmax()


rt_number = 0


def get_most_rt(retweets):
    global rt_number
    for retweet in retweets:
        rt_number = rt_number + retweet
    return retweets.idxmax()


def save_most_likes(row):
    most_liked = []
    details = ['name', 'screen_name', 'text', 'likes']
    for detail in details:
        most_liked.append(row[detail])
    most_liked.append(get_sentiment(clean_tweets(most_liked[2])))
    return most_liked


def save_most_retweets(row):
    most_retweets = []
    details = ['name', 'screen_name', 'text', 'retweets']
    for detail in details:
        most_retweets.append(row[detail])
    most_retweets.append(get_sentiment(clean_tweets(most_retweets[2])))
    return most_retweets


if __name__ == '__main__':
    logger.debug(df.head())
    try:
        date = get_date()
        study_update = study
        study += "_" + date.replace("-", "_")
        create_analyse_table(study, conn)
        # Preprocess and Clean tweets

        df['clean_text'] = df['text'].map(lambda tweet: clean_tweets(tweet))
        #
        # # Get the Sentiment and Compound score of each tweet and store it
        df[['compound_score', 'sentiment']] = df['clean_text'].apply(lambda tweet: sentiment_scores(tweet))
        #
        logger.debug(df['text'], df['sentiment'], df['compound_score'])
        logger.info("Number of tweets: " + str(number_of_tweets))
        logger.info("Number of positive tweets: " + str(positive_tweets))
        logger.info("Number of neutral tweets: " + str(neutral_tweets))
        logger.info("Number of negative tweets: " + str(negative_tweets))
        logger.info("Overall Compound Score is " + str(get_overall_score()))
        logger.info("Overall Sentiment is " + get_overall_sentiment())

        # Preprocess tweets and generate wordcloud
        df['clean_text'] = df['text'].map(lambda tweet: preprocess_tweets(tweet))
        word_cloud_list = wordcloud_list(df['clean_text'].str.lower())
        logger.debug(word_cloud_list)

        # # Clean Hashtags and generate wordcloud made up of most common hashtags
        df['hashes'] = df['hashes'].map(lambda hashtag: clean_hashtags(hashtag))
        hashtag_word_cloud = hashtag_wordcloud(df['hashes'].str.lower())
        logger.debug(hashtag_word_cloud)

        # Save verified users' names in table
        df.loc[(df['verified'] == 'True'), 'verified_users'] = df['name']
        df.loc[(df['verified'] == 'False'), 'verified_users'] = 'X'

        # Get % of every type of user
        user_type_percentages(df['verified_users'])
        logger.info("Percentage of undetermined users is " + str(percentage_of_other_users))
        logger.info("Percentage of companies users is " + str(percentage_of_companies))
        logger.info("Percentage of influences users is " + str(percentage_of_influencers))

        # Fetch wordcloud of companies
        wordcloud_companies = word_cloud_companies(df['verified_users'])
        logger.debug(wordcloud_companies)

        # Fetch list of coordinates tuples
        geo_coordinates = (get_list_of_coordinates(df['geo']))
        logger.debug(geo_coordinates)

        # Get most Liked tweet and number of likes
        df['likes'] = pd.to_numeric(df['likes'])
        most_liked_id = get_most_liked(df['likes'])
        row = df.iloc[most_liked_id]
        most_liked = save_most_likes(row)
        logger.debug(save_most_likes(row))
        logger.info("Number of likes = " + str(likes_number))

        # Get most Retweeted tweet and number of retweets
        df['retweets'] = pd.to_numeric(df['retweets'])
        most_retweets_id = get_most_rt(df['retweets'])
        row = df.iloc[most_retweets_id]
        most_retweet = save_most_retweets(row)
        most_retweet[2]=most_retweet[2].replace('amp;','')
        logger.debug(most_retweet)
        logger.info("Number of retweets = " + str(rt_number))

        insert_analysed_data(study_update, number_tweets=number_of_tweets, positive=positive_tweets,
                             neutral=neutral_tweets,
                             negative=negative_tweets, compound=get_overall_sentiment(), word_cloud=word_cloud_list,
                             company_cloud=wordcloud_companies, number_users=percentage_of_other_users,
                             companies=percentage_of_companies, number_influ=percentage_of_influencers,
                             hashtag_cloud=hashtag_word_cloud, countries_cloud=geo_coordinates, likes=likes_number,
                             retweets=rt_number, most_liked=most_liked, most_retweeted=most_retweet, conn=conn)
        subprocess.Popen(
            'python backend/GenderClassification.py {0}'.format(study_update),
            shell=True,
        )

    except Exception as e:
        logger.error("Error occurred in Analysing tweets {}".format(e.__str__()))

    finally:
        sys.exit(0)
