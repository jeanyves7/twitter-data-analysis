import psycopg2
from configparser import ConfigParser


def config():
    db = {}
    
    db[os.getenv('user')] = os.getenv('redshift')
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db


# method to clean the data from any inconvenient set of characters
def clean(liste, name, rep):
    for i in liste:
        name = name.replace(i, rep)
    return name


def connect(name):
    conn = None
    # cleaning the data
    name = clean([' ', '-'], name, '_')
    try:
        params = config()
        print('Connecting to Redshift...')
        conn = psycopg2.connect(**params)

        cur = conn.cursor()
        # creating the table for the related search
        query = "CREATE TABLE IF NOT EXISTS {0} ( id Varchar(256) Primary Key, Name VARCHAR(256),screen_name VARCHAR (256),retweets VARCHAR(256),likes varchar(256),description varchar(3000),verified varchar(255),geo varchar(255),hashes Varchar(2000),lang varchar(20),followers varchar(256),followingC varchar(256),text varchar (1500) )".format(
            name)
        cur.execute(query)
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
