from pymysql import Connection
import pandas as pd, subprocess

from .DatabaseConnection import clean
import datetime
import hashlib


def get_date():
    date = datetime.datetime.now().date()
    return str(date)


def get_available_search_engine(conn: Connection):
    cur = None
    try:
        query = "SELECT name from engine_search WHERE status='false'"
        df = pd.read_sql(sql=query, con=conn)
        data = df['name']
        if len(data) == 0:
            return None

        for i in df['name']:
            print(i)
        data = data[0]
        cur = conn.cursor()
        query = "Update engine_search set status='true' where name='{}'".format(data)
        cur.execute(query)
        conn.commit()

        return df['name'][0]

    except Exception as e:
        print("Error engine occurred in get_available_search_engine: {}".format(e.__str__()))
    finally:
        if cur is not None:
            cur.close()


def ongoing_search(conn: Connection, check_email: bool, name="", email="", ):
    # cleaning the data
    name = clean([' ', '-'], name, '_')
    cur = None
    print("checking for ongoing search for {0}".format(name))
    try:

        if check_email:
            query = "Select email from ongoing_search where email='{}'".format(email)
            df = pd.read_sql(sql=query, con=conn)
            data = df['email']

        else:
            query = "Select query from ongoing_search where query='{}'".format(name)
            df = pd.read_sql(sql=query, con=conn)
            data = df['query']
        if len(data) != 0:
            return True
        return False

    except Exception as e:
        print("Error occurred in ongoing_search : {}".format(e.__str__()))


def update_ongoing_search(name, conn: Connection, engine, email):
    name = clean([' ', '-'], name, '_')
    cur = None
    try:
        cur = conn.cursor()
        query = "Insert Into ongoing_search(engine,query,email) values('{0}','{1}','{2}')".format(engine, name, email)
        cur.execute(query)
        conn.commit()
        cur.close()
        return False
    except Exception as e:
        print("Error occurred in update_ongoing_search: {}".format(e.__str__()))
    finally:
        if cur is not None:
            cur.close()


def update_requested_study(study, insert: bool, conn: Connection, email=""):
    cur = None
    study = clean([' ', '-'], study, '_')
    try:
        cur = conn.cursor()
        query = None
        if insert:
            query = "INSERT INTO update_requested_study(email,study) values('{0}','{1}')".format(email, study)
        else:
            query = "DELETE FROM update_requested_study where study='{}'".format(study)
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print("Error in update_requested_study : {}".format(e.__str__()))
    finally:
        if cur is not None:
            cur.close()


def post_waiting_query(email, study, stop_date, conn: Connection):
    cur = None
    try:
        cur = conn.cursor()
        query = "Insert Into waiting_query(email,query,stop_date) values('{0}','{1}','{2}')".format(email, study,
                                                                                                    stop_date)
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print("Error occurred in post_waiting_query : {}".format(e.__str__()))
    finally:
        if cur is not None:
            cur.close()


def update_previous_study(study, report: bool, start: bool, conn: Connection):
    cur = None
    try:
        cur = conn.cursor()
        query = ""
        date = get_date()
        if report:
            query = "UPDATE previous_studies SET report_date='{0}' where study='{1}'".format(date, study)
            cur.execute(query)
            conn.commit()

        else:
            if start:
                query = "INSERT INTO previous_studies(study, start_date, end_date, report_date) VALUES ('{0}','{1}',NULL,NULL)".format(
                    study, date)
                cur.execute(query)
                conn.commit()
                query = "select id from previous_studies where study='{0}' and start_date='{1}'".format(study,
                                                                                                        get_date())
                df = pd.read_sql(query, conn)
                data = df.iloc[0]
                data = data[0]
                hashed = hashlib.sha256(int(data).to_bytes(16, 'little', signed=False)).hexdigest()
                query = "UPDATE previous_studies set hashed='{0}' where study='{1}'".format(str(hashed), study)
                cur.execute(query)
                conn.commit()

            else:
                query = "UPDATE previous_studies SET end_date='{0}' where study='{1}'".format(date, study)
                cur.execute(query)
                conn.commit()

    except Exception as e:
        print("Error occurred in update_previous_study : {}".format(e.__str__()))
    finally:
        if cur is not None:
            cur.close()


def get_waiting_query(conn: Connection):
    cur = None
    try:
        query = "SELECT * FROM waiting_query"
        df = pd.read_sql(sql=query, con=conn)
        if not df.empty:
            data = df.iloc[0]
            email = data[0]
            study = data[1]
            stop_date = data[2]
            cur = conn.cursor()
            query = "DELETE FROM waiting_query where email='{}'".format(email)
            cur.execute(query)
            conn.commit()
            per = get_available_search_engine(conn)
            print(per)
            update_ongoing_search(study, conn, per, email)
            update_requested_study(study=study, email=email, insert=True, conn=conn)
            update_previous_study(study=study, report=False, start=True, conn=conn)
            subprocess.Popen(
                'python backend/SearchTweets.py {} {} {} {}'.format(
                    study, email, stop_date, per
                ),
                shell=True,
            )
            print("opened query")

    except Exception as e:
        print("Error occurred in get_waiting_query : {}".format(e.__str__()))
    finally:
        if cur is not None:
            cur.close()


def close_engine_search(search_query, conn: Connection):
    cur = None
    search_query = clean([' ', '-'], search_query, '_')
    try:
        query = " Select engine from ongoing_search where query='{}' ".format(search_query)
        df = pd.read_sql(sql=query, con=conn)
        engine = df.iloc[0]
        print("closing engine:" + engine[0])
        cur = conn.cursor()
        query = "Update engine_search set status='false' where name='{}'".format(engine[0])
        cur.execute(query)
        conn.commit()
        query = "Delete from ongoing_search where query='{}'".format(search_query)
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print("Error occurred in close_engine_search {}".format(e.__str__()))
    finally:
        if cur is not None:
            cur.close()


def create_analyse_table(study, conn: Connection):
    study = clean([' ', '-'], study, '_')
    cur = None
    try:
        cur = conn.cursor()
        query = "CREATE TABLE IF NOT EXISTS {0} (study varchar(256),start_date varchar(20),end_date varchar(20),number_tweets bigint,positive integer ,neutral integer,negative integer ,compound varchar (40),word_cloud varchar (2000),company_cloud varchar(2000),number_users bigint,companies bigint,number_influ bigint,hashtag_cloud varchar(1500),countries_cloud varchar(3000),male bigint,female bigint,likes bigint,retweets bigint,most_liked varchar(1500),most_retweeted varchar(1500)) ".format(
            study)
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print("Error in create_analyse_table : {}".format(e.__str__()))
    finally:
        if cur is not None:
            cur.close()


def insert_analysed_data(table, number_tweets, positive, neutral, negative, compound, word_cloud, company_cloud,
                         number_users, companies, number_influ, hashtag_cloud, countries_cloud, likes, retweets,
                         most_liked, most_retweeted, conn: Connection):
    cur = None
    try:
        list_word = ""
        for i in word_cloud:
            if i[0] == table:
                continue
            list_word += "{} {}_".format(i[0], str(i[1]))
        company_word = ""

        for i in company_cloud:
            p = str(i[0]).replace("'", " ")
            company_word += "{} value:{}_".format(p, str(i[1]))

        country_word = ""
        counter = 0
        for i in countries_cloud:
            # max limit characters in the database column
            if counter == 220:
                break
            country_word += "{:.2f} {:.2f}_".format(float(i[0]), float(i[1]))
            counter += 1
        hash_word = ""
        for i in hashtag_cloud:
            if i[0] == table:
                continue
            hash_word += "{} {}_".format(i[0], str(i[1]))
        mp = str(most_liked[2]).replace("'", " ")
        most_liked_tweet = "{0}Mdp20Sep{1}Mdp20Sep{2}Mdp20Sep{3}".format(most_liked[1], mp, str(most_liked[3]),
                                                                         most_liked[4])
        mr = str(most_retweeted[2]).replace("'", " ")
        most_retweeted_tweet = "{0}Mdp20Sep{1}Mdp20Sep{2}Mdp20Sep{3}".format(most_retweeted[1], mr,
                                                                             str(most_retweeted[3]),
                                                                             most_retweeted[4])

        query = "SELECT start_date,end_date from previous_studies where study='{}'".format(table)
        df = pd.read_sql(query, conn)
        data = df.iloc[0]
        start = data[0]
        end_d = data[1]
        list_word = list_word[:-1]
        company_word = company_word[:-1]
        hash_word = hash_word[:-1]
        country_word = country_word[:-1]
        study_date = table + "_" + get_date().replace("-", "_")
        cur = conn.cursor()
        query = "INSERT INTO {0}(study,start_date,end_date,number_tweets,positive,neutral,negative,compound,word_cloud,company_cloud,number_users,companies,number_influ,hashtag_cloud,countries_cloud,likes,retweets,most_liked,most_retweeted)VALUES ('{1}','{2}','{3}',{4},{5},{6},{7},'{8}','{9}','{10}',{11},{12},{13},'{14}','{15}',{16},{17},'{18}','{19}')".format(
            study_date, table, start, end_d, number_tweets, positive, neutral, negative, compound, list_word,
            company_word,
            number_users,
            companies, number_influ, hash_word, country_word, likes, retweets, most_liked_tweet, most_retweeted_tweet)
        cur.execute(query)
        conn.commit()

    except Exception as e:
        print("Error occurred in insert_analysed_data: {}".format(e))
    finally:
        if cur is not None:
            cur.close()


def update_gender_percentage(study, male, female, conn: Connection):
    cur = None
    try:
        study_date = study + "_" + get_date().replace("-", "_")
        query = "Update {0} set male={1}, female={2} where study='{3}'".format(study_date, male, female, study)
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print("Error occurred in update_gender_percentage {}".format(e.__str__()))
    finally:
        if cur is not None:
            cur.close()


def get_previous_studies(conn: Connection):
    try:
        query = "select hashed,study,start_date,end_date from previous_studies where report_date IS NOT NULL order by start_date"
        df = pd.read_sql(sql=query, con=conn)
        list_data = []
        for i in range(len(df)):
            data = df.iloc[i]
            id_study = data[0]
            study = data[1]
            start_date = data[2]
            end_date = data[3]
            d = {"id": str(id_study),
                 "study": study,
                 "start_date": start_date,
                 "end_date": end_date
                 }
            list_data.append(d)
        j_data = {
            "data": list_data
        }
        return j_data
    except Exception as e:
        print("Error occurred in get_previous_studies: {}".format(e.__str__()))


def get_hash_id(study, conn: Connection):
    try:
        query = "SELECT hashed from previous_studies where study='{0}'".format(study)
        df = pd.read_sql(query, conn)
        data = df.iloc[0][0]
        return data
    except Exception as e:
        print("Error occurred in get_hash_id: {0}".format(e.__str__()))


def split_words(cloud, sep=" ", first="text", second="value"):
    try:
        if (len(cloud)) == 1:
            if cloud[0] == '':
                return cloud
        list_word = []
        for i in cloud:
            word = i.split(sep)
            print(word)
            print("the words : {}".format(word))
            j_format = {first: str(word[0]), second: str(word[1])}
            list_word.append(j_format)
        return list_word
    except Exception as e:
        print("Error occurred with {} as {}".format(cloud, e.__str__()))


def get_analysed_study(study_id, conn: Connection):
    conn.commit()
    try:
        query = "select study,report_date from previous_studies where hashed='{}' and report_date is not NULL".format(
            str(study_id))
        df = pd.read_sql(query, conn)
        report_date = str(df.iloc[0][1])
        study = str(df.iloc[0][0]) + "_" + report_date.replace("-", "_")
        query = "Select * from {}".format(study)
        df = pd.read_sql(query, conn)
        data = df.iloc[0]

        name = data[0]
        start = data[1]
        end = data[2]
        tweets_number = str(data[3])
        positive = str(data[4])
        neutral = str(data[5])
        negative = str(data[6])
        compound = data[7]

        print("word_cloud")
        word_cloud = str(data[8]).split("_")
        list_word = split_words(word_cloud)

        print("company_cloud")
        company_cloud = str(data[9]).split("_")
        company_word = split_words(company_cloud, sep=" value:")

        number_users = str(data[10])
        company = str(data[11])
        influ = str(data[12])

        print("hash_cloud")
        hash_cloud = str(data[13]).split("_")
        hash_word = split_words(hash_cloud)

        print("countries")
        countries_cloud = str(data[14]).split("_")
        countries_word = split_words(countries_cloud, first="long", second="lat")

        male = str(data[15])
        female = str(data[16])
        gender = {"male": male, "female": female}

        likes = str(data[17])
        retweets = str(data[18])

        most_liked = str(data[19]).split("Mdp20Sep")
        print(most_liked)
        most_liked_tweet = {"userName": most_liked[0],
                            "tweet_text": most_liked[1],
                            "number_of_likes": most_liked[2],
                            "sentiment_score": most_liked[3]}

        most_retweeted = str(data[20]).split("Mdp20Sep")
        print(most_retweeted)
        most_retweeted_tweet = {"userName": most_retweeted[0],
                                "tweet_text": most_retweeted[1],
                                "number_of_likes": most_retweeted[2],
                                "sentiment_score": most_retweeted[3]}

        j_data = {
            "data": {
                "study_name": name,
                "start_date": start,
                "end_date": end,
                "tweets_number": tweets_number,
                "positive": positive,
                "neutral": neutral,
                "negative": negative,
                "compound": compound,
                "words": list_word,
                "company_word": company_word,
                "number_users": number_users,
                "company": company,
                "influ": influ,
                "hashtag_cloud": hash_word,
                "countries_coordinates": countries_word,
                "gender_percentage": gender,
                "number_of_likes": likes,
                "number_of_retweets": retweets,
                "most_liked_tweet": most_liked_tweet,
                "most_retweeted_tweet": most_retweeted_tweet
            }
        }
        return j_data
    except Exception as e:
        print("Error occurred in get_analysed_study: {}".format(e.__str__()))
