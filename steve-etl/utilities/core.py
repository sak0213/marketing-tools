from utilities.cred import *
import psycopg2


class PostgresConnection:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=pg_database_name,
            user=pg_database_user,
            host=pg_host,
            password=pg_password)
        self.cur = self.conn.cursor()

