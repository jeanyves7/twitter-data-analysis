import psycopg2
from configparser import ConfigParser
import os
import logging

logger = logging.getLogger(__name__)


def config(filename='database.ini', section='postgresql'):
    """Load database connection settings.

    Preference is given to environment variables; if a configuration
    file exists and contains the requested section it will be used
    instead.  The return value is a dictionary compatible with
    ``psycopg2.connect``.
    """

    db = {}

    if os.path.isfile(filename):
        parser = ConfigParser()
        parser.read(filename)

        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception(
                'Section {0} not found in the {1} file'.format(section, filename)
            )
    else:
        db = {
            'host': os.getenv('host_RDS'),
            'user': os.getenv('RDS_user'),
            'password': os.getenv('RDS_pass'),
            'port': os.getenv('RDS_port'),
            'dbname': os.getenv('RDS_db'),
        }

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
        logger.info('Connecting to Redshift...')
        conn = psycopg2.connect(**params)

        cur = conn.cursor()
        # creating the table for the related search
        query = "CREATE TABLE IF NOT EXISTS {0} ( id Varchar(256) Primary Key, Name VARCHAR(256),screen_name VARCHAR (256),retweets VARCHAR(256),likes varchar(256),description varchar(3000),verified varchar(255),geo varchar(255),hashes Varchar(2000),lang varchar(20),followers varchar(256),followingC varchar(256),text varchar (1500) )".format(
            name)
        cur.execute(query)
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
    finally:
        if conn is not None:
            conn.close()
            logger.info('Database connection closed.')
