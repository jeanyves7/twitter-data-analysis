import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import LabelEncoder
import re
from .sendMail import send_mail
import regex
import nltk, sys
from sqlalchemy import create_engine
import psycopg2
import os
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from sklearn.naive_bayes import MultinomialNB
import joblib
from .Rds_Handle import (
    get_date,
    create_analyse_table,
    insert_analysed_data,
    update_previous_study,
    update_gender_percentage,
)
from .app import conn

# nltk.download('stopwords')

engine = create_engine(os.getenv("REDSHIFT_URL"))
q = sys.argv

# study = 'amazon'
study=q[1]

query = "SELECT * FROM {}".format(study)

df = pd.read_sql(sql=query, con=engine)

data = pd.read_csv('./ClassificationDataSet.csv', encoding='latin-1')

unknown_items_idx = data[data['gender'] == 'unknown'].index
data.drop(index=unknown_items_idx, inplace=True)
drop_items_idx = data[data['gender:confidence'] < 1].index
data.drop(index=drop_items_idx, inplace=True)
data.drop(columns=['gender:confidence'], inplace=True)

stop = stopwords.words('english')
porter = PorterStemmer()


def preprocessor(text):
    text = re.sub('<[^>]*>', '', text)
    text = re.sub('http.*', ' ', text)
    emoticons = re.findall('(?::|;|=)(?:-)?(?:\)|\(|D|P)', text)
    text = re.sub('[^a-zA-Z0-9]+', ' ', text.lower()) + ' ' + ' '.join(emoticons).replace('-', '')
    return text


def remove_whitespace(text):
    return re.sub('\s{2,}', ' ', text)


def tokenizer_porter(text):
    return [porter.stem(word) for word in text.lower().split()]


def clean_tweet(text):
    clean = ""
    tokens = tokenizer_porter(text)
    for token in tokens:
        if len(token) > 1:
            if token not in stop:
                clean += preprocessor(token)
    return remove_whitespace(clean)


female = 0
male = 0
unknown = 0


def get_gender_nb(genders):
    global female
    global male
    global unknown
    for gender in genders:
        if gender == 1:
            female += 1
        elif gender == 2:
            male += 1
        else:
            unknown += 1


female_percentage = 0
male_percentage = 0


def get_gender_percentage():
    global female
    global male
    global female_percentage
    global male_percentage
    users = female + male
    female_percentage = (female / users) * 100
    male_percentage = (male / users) * 100
    return users


def has_nan(description):
    description = description.isnull()
    description = description.add_suffix('_has_nan')
    return description


if __name__ == '__main__':
    has_nan_data = has_nan(data[['description']])
    data = pd.concat([data, has_nan_data], axis=1)
    data['description'].fillna("", inplace=True)

    has_nan_df = has_nan(df[['description']])
    df = pd.concat([df, has_nan_df], axis=1)
    df['description'].fillna("", inplace=True)

    data['text'] = data['text'].map(lambda text: clean_tweet(text))
    data['description'] = data['description'].map(lambda description: clean_tweet(description))
    data['text_description'] = data['text'].str.cat(data['description'], sep=' ')

    df['text'] = df['text'].map(lambda text: clean_tweet(text))
    df['description'] = df['description'].map(lambda description: clean_tweet(description))
    df['text_description'] = df['text'].str.cat(df['description'], sep=' ')

    tfidf = TfidfVectorizer()
    tfidf = tfidf.fit(data['text_description'])
    encoder = LabelEncoder()

    y_labeled = encoder.fit_transform(data['gender'])
    X_labeled = tfidf.transform(data['text_description'])

    X_unlabeled = tfidf.transform(df['text_description'])

    nb = MultinomialNB()
    nb = nb.fit(X_labeled, y_labeled)

    filename = './gender_classification_model.sav'
    joblib.dump(nb, filename)

    df['gender'] = nb.predict(X_unlabeled)

    get_gender_nb(df['gender'])
    print("Number of users: ")
    nb = get_gender_percentage()
    print(str(nb))
    print("female and male numbers: " + str(female) + ", " + str(male))
    print("female and male percentage: " + str(female_percentage) + ", " + str(male_percentage))
    update_gender_percentage(study=study, male=male_percentage, female=female_percentage, conn=conn)
    update_previous_study(study, report=True, start=False, conn=conn)
    send_mail(study)
