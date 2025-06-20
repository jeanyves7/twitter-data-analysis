import os
import psycopg2

host = os.getenv('POSTGRES_HOST', os.getenv('host_RDS'))
PORT = int(os.getenv('POSTGRES_PORT', os.getenv('RDS_port') or 5432))
USER = os.getenv('POSTGRES_USER', os.getenv('RDS_user'))
PASS = os.getenv('POSTGRES_PASSWORD', os.getenv('RDS_pass'))
DBNAME = os.getenv('POSTGRES_DB', os.getenv('RDS_db'))
conn = psycopg2.connect(host=host, user=USER, password=PASS, port=PORT, dbname=DBNAME)
