import os
import pymysql

host = os.getenv('host_RDS')
PORT = int(os.getenv('RDS_port'))
USER = os.getenv('RDS_user')
PASS = os.getenv('RDS_pass')
DBNAME = os.getenv('RDS_db')

conn = pymysql.connect(host=host, user=USER, port=PORT, passwd=PASS, database=DBNAME)
